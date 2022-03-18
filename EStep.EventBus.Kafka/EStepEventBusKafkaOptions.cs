namespace EStep.EventBus.Kafka;

public class EStepEventBusKafkaOptions {
  /// <summary>
  ///   连接名称
  /// </summary>
  public string ConnectionName { get; set; } = null!;

  /// <summary>
  ///   消费topic是否与租户信息相关
  ///   格式:ConsumeTopicName-{tenantName}
  /// </summary>
  public bool IsConsumeTopicLinkTenant { get; set; }

  /// <summary>
  ///   消费Topic
  /// </summary>
  public string ConsumeTopicName { get; set; } = null!;

  /// <summary>
  ///   租户名称
  /// </summary>
  public string TenantName { get; set; } = null!;

  /// <summary>
  ///   消费组名
  /// </summary>
  public string GroupId { get; set; } = null!;

  /// <summary>
  ///   生产topic是否与租户信息相关
  ///   格式:ProduceTopicName-{tenantName}
  /// </summary>
  public bool IsProduceTopicLinkTenant { get; set; }

  /// <summary>
  ///   生产topic的租户是否来自业务的当前租户
  /// </summary>
  public bool IsProduceTopicLinkCurrentTenant { get; set; }

  /// <summary>
  ///   生产Topic
  /// </summary>
  public string ProduceTopicName { get; set; } = null!;
}