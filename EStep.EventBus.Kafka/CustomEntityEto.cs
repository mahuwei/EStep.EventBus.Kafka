using Volo.Abp.Domain.Entities.Events.Distributed;

namespace EStep.EventBus.Kafka;

public class CustomEntityEto : EntityEto {
  public CustomEntityEto(string entityType, string keysAsString, object value) : base(entityType,
    keysAsString) {
    Value = value;
  }

  public object Value { get; set; }
}