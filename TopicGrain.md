``` c#

//o IGrainWithIntegerCompoundKey

public interface ITopicGrain : IGrainWithStringKey
{
    Task Subscribe(ISensorGrain sensor);
    Task Unsubscribe(ISensorGrain sensor);
    Task Publish(SensorReading reading);
}

public sealed class TopicState
{
    public HashSet<string> SubscriberIds { get; set; } = [];
}

public sealed class TopicGrain : Grain, ITopicGrain
{
    private readonly HashSet<ISensorGrain> _subscribers = [];

    public Task Subscribe(ISensorGrain sensor)
    {
        _subscribers.Add(sensor);
        return Task.CompletedTask;
    }

    public Task Unsubscribe(ISensorGrain sensor)
    {
        _subscribers.Remove(sensor);
        return Task.CompletedTask;
    }

    // public async Task Publish(SensorReading reading)
    // {
    //     var tasks = _subscribers.Select(s => s.ProcessReading(reading));
    //     await Task.WhenAll(tasks);
    // }

    public async Task Publish(SensorReading reading)
    {
        var tasks = _subscriberIds
            .Select(id => GrainFactory.GetGrain<ISensorGrain>(id))
            .Select(sensor => sensor.ProcessReading(reading));

        await Task.WhenAll(tasks);
    }
}

```
