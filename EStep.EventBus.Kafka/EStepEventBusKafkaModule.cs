using Microsoft.Extensions.DependencyInjection;
using Volo.Abp;
using Volo.Abp.EventBus;
using Volo.Abp.Kafka;
using Volo.Abp.Modularity;

namespace EStep.EventBus.Kafka;

[DependsOn(typeof(AbpEventBusModule), typeof(AbpKafkaModule))]
public class EStepEventBusKafkaModule : AbpModule {
  override public void ConfigureServices(ServiceConfigurationContext context) {
    Configure<EStepEventBusKafkaOptions>(context.Services.GetConfiguration()
      .GetSection("Kafka:EventBus"));
  }

  override public void OnApplicationInitialization(ApplicationInitializationContext context) {
    context.ServiceProvider.GetRequiredService<EStepKafkaDistributedEventBus>().Initialize();
  }
}