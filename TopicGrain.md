``` c#

//o IGrainWithIntegerCompoundKey

public interface ITopicGrain : IGrainWithStringKey
{
    Task Subscribe(IAddressable grain);
    Task Unsubscribe(IAddressable grain);
    Task Publish(AtlasElementProjection reading);
}

public sealed class TopicState
{
    public HashSet<long> SectorIds { get; set; } = [];
    public HashSet<long> HydrantIds { get; set; } = [];
    public HashSet<long> CheckpointIds { get; set; } = [];

    public void AddSubscription()
    {

    }
}

public sealed class TopicGrain : Grain, ITopicGrain
{
    private readonly HashSet<ISensorGrain> _subscribers = [];
    private readonly IState<TopicState> _state;

    public Task Subscribe(ISensorGrain sensor)
    {
        _subscribers.Add(sensor);

        if (!_state.State.Exist(sensor.Id))
            _state.State.Add(sensor.Id)

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

// ====================
public interface ISectorGrain : IGrainWithIntegerKey { }
public interface IHydrantGrain : IGrainWithIntegerKey { }
public interface ICheckpointGrain : IGrainWithIntegerKey { }

public sealed class TopicState
{
    public HashSet<long> SectorIds { get; set; } = [];
    public HashSet<long> HydrantIds { get; set; } = [];
    public HashSet<long> CheckpointIds { get; set; } = [];

    public void AddSubscription(IAddressable grain)
    {
        switch (grain)
        {
            case ISectorGrain sector:
                SectorIds.Add(sector.GetPrimaryKeyLong());
                break;

            case IHydrantGrain hydrant:
                HydrantIds.Add(hydrant.GetPrimaryKeyLong());
                break;

            case ICheckpointGrain checkpoint:
                CheckpointIds.Add(checkpoint.GetPrimaryKeyLong());
                break;

            default:
                throw new InvalidOperationException(
                    $"Unsupported subscription grain type: {grain.GetType().Name}");
        }
    }
}





public sealed class SectorGrain : Grain, ISectorGrain
{
    public async Task SubscribeToTopic(long topicId, string topicName)
    {
        var topic = GrainFactory.GetGrain<ITopicGrain>(
            primaryKey: topicId,
            keyExtension: topicName);

        var self = this.AsReference<ISectorGrain>();

        await topic.Subscribe(self);
    }
}


```



 