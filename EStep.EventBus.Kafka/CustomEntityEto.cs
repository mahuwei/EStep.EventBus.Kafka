using Volo.Abp.Domain.Entities.Events.Distributed;

namespace EStep.EventBus.Kafka;

public class CustomEntityEto : EntityEto {
  public CustomEntityEto(string entityType, string keysAsString,string lastChanged) : base(entityType,
    keysAsString) {
    LastChanged = lastChanged;
  }

  /// <summary>
  ///   最后变更时间
  /// </summary>
  public string LastChanged { get; set; }
}