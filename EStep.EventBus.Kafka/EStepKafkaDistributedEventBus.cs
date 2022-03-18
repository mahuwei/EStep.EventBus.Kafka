using System.Collections.Concurrent;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Volo.Abp;
using Volo.Abp.DependencyInjection;
using Volo.Abp.EventBus;
using Volo.Abp.EventBus.Distributed;
using Volo.Abp.Guids;
using Volo.Abp.Kafka;
using Volo.Abp.MultiTenancy;
using Volo.Abp.Threading;
using Volo.Abp.Timing;
using Volo.Abp.Uow;

namespace EStep.EventBus.Kafka;

[Dependency(ReplaceServices = true)]
[ExposeServices(typeof(IDistributedEventBus), typeof(EStepKafkaDistributedEventBus))]
public class EStepKafkaDistributedEventBus : DistributedEventBusBase, ISingletonDependency {
  public EStepKafkaDistributedEventBus(IServiceScopeFactory serviceScopeFactory,
    ICurrentTenant currentTenant,
    IUnitOfWorkManager unitOfWorkManager,
    IOptions<EStepEventBusKafkaOptions> abpKafkaEventBusOptions,
    IKafkaMessageConsumerFactory messageConsumerFactory,
    IOptions<AbpDistributedEventBusOptions> abpDistributedEventBusOptions,
    IKafkaSerializer serializer,
    IProducerPool producerPool,
    IGuidGenerator guidGenerator,
    IClock clock,
    IEventHandlerInvoker eventHandlerInvoker)
    : base(serviceScopeFactory, currentTenant, unitOfWorkManager, abpDistributedEventBusOptions,
      guidGenerator, clock, eventHandlerInvoker) {
    EStepEventBusKafkaOptions = abpKafkaEventBusOptions.Value;
    MessageConsumerFactory = messageConsumerFactory;
    Serializer = serializer;
    ProducerPool = producerPool;
    HandlerFactories = new ConcurrentDictionary<Type, List<IEventHandlerFactory>>();
    EventTypes = new ConcurrentDictionary<string, Type>();
  }

  protected EStepEventBusKafkaOptions EStepEventBusKafkaOptions { get; }

  protected IKafkaMessageConsumerFactory MessageConsumerFactory { get; }

  protected IKafkaSerializer Serializer { get; }

  protected IProducerPool ProducerPool { get; }

  protected ConcurrentDictionary<Type, List<IEventHandlerFactory>> HandlerFactories { get; }

  protected ConcurrentDictionary<string, Type> EventTypes { get; }

  protected IKafkaMessageConsumer Consumer { get; private set; } = null!;

  public void Initialize() {
    var consumeTopicName = EStepEventBusKafkaOptions.ConsumeTopicName;
    if (EStepEventBusKafkaOptions.IsConsumeTopicLinkTenant) {
      if (string.IsNullOrWhiteSpace(EStepEventBusKafkaOptions.ConsumeTopicName)) {
        throw new ArgumentNullException(nameof(EStepEventBusKafkaOptions.TenantName));
      }

      consumeTopicName += $"-{EStepEventBusKafkaOptions.TenantName.ToLower()}";
    }

    Consumer = MessageConsumerFactory.Create(consumeTopicName,
      EStepEventBusKafkaOptions.GroupId, EStepEventBusKafkaOptions.ConnectionName);
    Consumer.OnMessageReceived(ProcessEventAsync);
    SubscribeHandlers(AbpDistributedEventBusOptions.Handlers);
  }

  private async Task ProcessEventAsync(Message<string, byte[]> message) {
    var distributedEventBus = this;
    var key = message.Key;
    var eventType = distributedEventBus.EventTypes.GetOrDefault<string, Type>(key);
    if (eventType == null) {
      eventType = null;
    }
    else {
      string? messageId = null;
      if (message.Headers.TryGetLastBytes("messageId", out var lastHeader)) {
        messageId = Encoding.UTF8.GetString(lastHeader);
      }

      if (await distributedEventBus.AddToInboxAsync(messageId, key, eventType, message.Value)
            .ConfigureAwait(false)) {
        eventType = null;
      }
      else {
        var eventData = distributedEventBus.Serializer.Deserialize(message.Value, eventType);
        await distributedEventBus.TriggerHandlersAsync(eventType, eventData).ConfigureAwait(false);
        eventType = null;
      }
    }
  }

  override public IDisposable Subscribe(Type eventType, IEventHandlerFactory factory) {
    var handlerFactories = GetOrCreateHandlerFactories(eventType);
    if (factory.IsInFactories(handlerFactories)) {
      return NullDisposable.Instance;
    }

    handlerFactories.Add(factory);
    return new EventHandlerFactoryUnregistrar(this, eventType, factory);
  }

  /// <inheritdoc />
  override public void Unsubscribe<TEvent>(Func<TEvent, Task> action) {
    Check.NotNull<Func<TEvent, Task>>(action, nameof(action));
    GetOrCreateHandlerFactories(typeof(TEvent))
      .Locking<List<IEventHandlerFactory>>((Action<List<IEventHandlerFactory>>)(factories =>
        factories.RemoveAll(factory =>
          factory is SingleInstanceHandlerFactory {
            HandlerInstance: ActionEventHandler<TEvent> handlerInstance
          }
          && handlerInstance.Action == action)));
  }

  /// <inheritdoc />
  override public void Unsubscribe(Type eventType, IEventHandler handler) {
    GetOrCreateHandlerFactories(eventType)
      .Locking<List<IEventHandlerFactory>>((Action<List<IEventHandlerFactory>>)(factories =>
        factories.RemoveAll(factory =>
          factory is SingleInstanceHandlerFactory instanceHandlerFactory
          && instanceHandlerFactory.HandlerInstance == handler)));
  }

  /// <inheritdoc />
  override public void Unsubscribe(Type eventType, IEventHandlerFactory factory) {
    GetOrCreateHandlerFactories(eventType)
      .Locking<List<IEventHandlerFactory>, bool>(
        (Func<List<IEventHandlerFactory>, bool>)(factories => factories.Remove(factory)));
  }

  /// <inheritdoc />
  override public void UnsubscribeAll(Type eventType) {
    GetOrCreateHandlerFactories(eventType)
      .Locking<List<IEventHandlerFactory>>(
        (Action<List<IEventHandlerFactory>>)(factories => factories.Clear()));
  }

  protected override async Task PublishToEventBusAsync(Type eventType, object eventData) {
    var eventType1 = eventType;
    var eventData1 = eventData;
    var headers = new Headers {
      { "messageId", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N")) }
    };
    await PublishAsync(eventType1, eventData1, headers, null).ConfigureAwait(false);
  }

  protected override void AddToUnitOfWork(IUnitOfWork unitOfWork,
    UnitOfWorkEventRecord eventRecord) {
    unitOfWork.AddOrReplaceDistributedEvent(eventRecord);
  }

  override public Task PublishFromOutboxAsync(OutgoingEventInfo outgoingEvent,
    OutboxConfig outboxConfig) {
    var topicName = GetProductTopicName();
    var eventName = outgoingEvent.EventName;
    var eventData = outgoingEvent.EventData;
    var headers = new Headers {
      { "messageId", Encoding.UTF8.GetBytes(outgoingEvent.Id.ToString("N")) }
    };
    return PublishAsync(topicName, eventName, eventData, headers, null);
  }

  override async public Task ProcessFromInboxAsync(IncomingEventInfo incomingEvent,
    InboxConfig inboxConfig) {
    var distributedEventBus = this;
    var eventType =
      distributedEventBus.EventTypes.GetOrDefault<string, Type>(incomingEvent.EventName);
    List<Exception>? exceptions;
    if (eventType == null) {
      eventType = null;
      exceptions = null;
    }
    else {
      var eventData =
        distributedEventBus.Serializer.Deserialize(incomingEvent.EventData, eventType);
      exceptions = new List<Exception>();
      await distributedEventBus.TriggerHandlersAsync(eventType, eventData, exceptions, inboxConfig)
        .ConfigureAwait(false);
      if (!exceptions.Any<Exception>()) {
        eventType = null;
        exceptions = null;
      }
      else {
        distributedEventBus.ThrowOriginalExceptions(eventType, exceptions);
        eventType = null;
        exceptions = null;
      }
    }
  }

  protected override byte[] Serialize(object eventData) {
    return Serializer.Serialize(eventData);
  }

  virtual async public Task PublishAsync(Type eventType,
    object eventData,
    Headers headers,
    Dictionary<string, object>? headersArguments) {
    var topicName = GetProductTopicName();

    await PublishAsync(topicName, eventType, eventData, headers,
        headersArguments)
      .ConfigureAwait(false);
  }

  private string GetProductTopicName() {
    if (string.IsNullOrWhiteSpace(EStepEventBusKafkaOptions.ProduceTopicName)) {
      throw new ArgumentNullException(nameof(Kafka.EStepEventBusKafkaOptions.ProduceTopicName));
    }

    var topicName = EStepEventBusKafkaOptions.ProduceTopicName;
    if (!EStepEventBusKafkaOptions.IsProduceTopicLinkTenant) {
      return topicName;
    }

    // 不与业务当前租户相关
    if (!EStepEventBusKafkaOptions.IsProduceTopicLinkCurrentTenant) {
      return $"{topicName}-{EStepEventBusKafkaOptions.TenantName}";
    }

    if (CurrentTenant.IsAvailable) {
      topicName += $"-{CurrentTenant.Name.ToLower()}";
    }

    return topicName;
  }

  private Task PublishAsync(string topicName,
    Type eventType,
    object eventData,
    Headers headers,
    Dictionary<string, object>? headersArguments) {
    var nameOrDefault = EventNameAttribute.GetNameOrDefault(eventType);
    var body = Serializer.Serialize(eventData);
    return PublishAsync(topicName, nameOrDefault, body, headers, headersArguments);
  }

  private async Task PublishAsync(string topicName,
    string eventName,
    byte[] body,
    Headers headers,
    Dictionary<string, object>? headersArguments) {
    IProducer<string, byte[]> producer = ProducerPool.Get(EStepEventBusKafkaOptions.ConnectionName);
    SetEventMessageHeaders(headers, headersArguments);
    var str = topicName;
    var message = new Message<string, byte[]> { Key = eventName, Value = body, Headers = headers };
    var cancellationToken = new CancellationToken();
    await producer.ProduceAsync(str, message, cancellationToken).ConfigureAwait(false);
  }

  private void SetEventMessageHeaders(Headers headers,
    Dictionary<string, object>? headersArguments) {
    if (headersArguments == null) {
      return;
    }

    foreach (var headersArgument in headersArguments) {
      headers.Remove(headersArgument.Key);
      headers.Add(headersArgument.Key, Serializer.Serialize(headersArgument.Value));
    }
  }

  private List<IEventHandlerFactory> GetOrCreateHandlerFactories(Type eventType) {
    return HandlerFactories.GetOrAdd(eventType, type => {
      EventTypes[EventNameAttribute.GetNameOrDefault(type)] = type;
      return new List<IEventHandlerFactory>();
    });
  }

  protected override IEnumerable<EventTypeWithEventHandlerFactories> GetHandlerFactories(
    Type eventType) {
    var handlerFactoriesList = new List<EventTypeWithEventHandlerFactories>();
    foreach (var keyValuePair in HandlerFactories
               .Where<KeyValuePair<Type, List<IEventHandlerFactory>>>(hf =>
                 ShouldTriggerEventForHandler(eventType, hf.Key))) {
      handlerFactoriesList.Add(
        new EventTypeWithEventHandlerFactories(keyValuePair.Key, keyValuePair.Value));
    }

    return handlerFactoriesList.ToArray();
  }

  private static bool ShouldTriggerEventForHandler(Type targetEventType, Type handlerEventType) {
    return handlerEventType == targetEventType
           || handlerEventType.IsAssignableFrom(targetEventType);
  }
}