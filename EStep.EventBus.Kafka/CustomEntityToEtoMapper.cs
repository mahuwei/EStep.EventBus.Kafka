using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Volo.Abp;
using Volo.Abp.DependencyInjection;
using Volo.Abp.Domain.Entities;
using Volo.Abp.Domain.Entities.Events.Distributed;
using Volo.Abp.DynamicProxy;
using Volo.Abp.ObjectMapping;

namespace EStep.EventBus.Kafka;

[Dependency(ReplaceServices = true)]
[ExposeServices(typeof(IEntityToEtoMapper))]
public class CustomEntityToEtoMapper : IEntityToEtoMapper, ITransientDependency {
  public CustomEntityToEtoMapper(IOptions<AbpDistributedEntityEventOptions> options,
    IServiceScopeFactory hybridServiceScopeFactory) {
    HybridServiceScopeFactory = hybridServiceScopeFactory;
    Options = options.Value;
  }

  protected IServiceScopeFactory HybridServiceScopeFactory { get; }

  protected AbpDistributedEntityEventOptions Options { get; }

  public object? Map(object entityObj) {
    Check.NotNull<object>(entityObj, nameof(entityObj));
    if (!(entityObj is IEntity entity)) {
      return null;
    }

    var type1 = ProxyHelper.UnProxy(entity).GetType();
    var orDefault = Options.EtoMappings.GetOrDefault<Type, EtoMappingDictionaryItem>(type1);
    if (orDefault == null) {
      var keysAsString = entity.GetKeys().JoinAsString<object>(",");
      return new CustomEntityEto(type1.FullName!, keysAsString, entityObj);
    }

    using var scope = HybridServiceScopeFactory.CreateScope();
    var type2 = !(orDefault.ObjectMappingContextType == null)
      ? typeof(IObjectMapper<>).MakeGenericType(orDefault.ObjectMappingContextType)
      : typeof(IObjectMapper);

    var type3 = type2;
    return ((IObjectMapper)scope.ServiceProvider.GetRequiredService(type3)).Map(type1,
      orDefault.EtoType, entityObj);
  }
}