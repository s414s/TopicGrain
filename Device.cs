using Orleans.BroadcastChannel;

namespace WebApplication3.Devices;

[ImplicitChannelSubscription(ChannelNames.DevicesNamespace)]
[KeepAlive]
public sealed class DeviceGrain : Grain, IDeviceGrain, IOnBroadcastChannelSubscribed, INotificationSubscriberGrain
{
    private DeviceState _state = new();

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine($"Device grain activated: {this.GetPrimaryKeyString()}");

        var grain = GrainFactory.GetGrain<ITopicGrain>("myTopic");

        var self = this.AsReference<ISectorGrain>();

        _state = new DeviceState
        {
            Name = "Initial",
            Status = "OFFline",
            LastUpdated = DateTimeOffset.UtcNow.AddDays(-1),
        };

        grain.Subscribe(self);

        return Task.CompletedTask;
    }

    public Task<DeviceState> GetStateAsync()
    {
        return Task.FromResult(_state);
    }

    public Task OnSubscribed(IBroadcastChannelSubscription subscription) =>
       subscription.Attach<DeviceStatusChanged>(OnChange, OnError);

    private Task OnChange(DeviceStatusChanged info)
    {
        var self = this.AsReference<ISectorGrain>();

        Console.WriteLine($"Calling broadcast from {this.GetGrainId()}");

        return Task.CompletedTask;
    }

    private static Task OnError(Exception ex)
    {
        Console.Error.WriteLine($"An error occurred: {ex}");

        return Task.CompletedTask;
    }

    public Task SetNameAsync(string name)
    {
        _state.Name = name;
        return Task.CompletedTask;
    }

    public Task UpdateStatusAsync(string status)
    {
        _state.Status = status;
        return Task.CompletedTask;
    }

    public Task ReceiveNotification(DeviceState state)
    {
        //Console.WriteLine($"Receving notification on Device grain {this.GetPrimaryKeyString()}");
        Console.WriteLine($"{this.GetPrimaryKeyString()}: Name={_state.Name}->{state.Name}, Status={_state.Status}->{state.Status}, LastUpdated={_state.LastUpdated}->{state.LastUpdated}");
        return Task.CompletedTask;
    }
}

public interface IDeviceGrain : IGrainWithGuidKey
{
    Task<DeviceState> GetStateAsync();
    Task SetNameAsync(string name);
    Task UpdateStatusAsync(string status);
}

//public interface INotificationSubscriberGrain : IGrainWithIntegerKey // WARNING - error ; in practice, orleans will create an id automatically
public interface INotificationSubscriberGrain : IGrainWithGuidKey
{
    Task ReceiveNotification(DeviceState state);
}

public interface ISectorGrain : IGrainWithIntegerKey { }
public interface ICheckpointGrain : IGrainWithIntegerKey { }
public interface IHydrantGrain : IGrainWithIntegerKey { }

[GenerateSerializer]
public sealed record DeviceState
{
    [Id(0)] public string Name { get; set; } = "Unnamed device";
    [Id(1)] public string Status { get; set; } = "Offline";
    [Id(2)] public DateTimeOffset LastUpdated { get; set; } = DateTimeOffset.UtcNow;

    [Id(3)] public List<long> SectorIds { get; set; } = [];
    [Id(4)] public List<long> HydrantIds { get; set; } = [];
    [Id(5)] public List<long> CheckpointIds { get; set; } = [];

    public DeviceState Subscribe(IAddressable grain)
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
                throw new InvalidOperationException($"Unsupported subscription grain type: {grain.GetType().Name}");
        }
        return this;
    }

    public void Unsubscribe(IAddressable grain)
    {
        switch (grain)
        {
            case ISectorGrain sector:
                SectorIds.Remove(sector.GetPrimaryKeyLong());
                break;

            case IHydrantGrain hydrant:
                HydrantIds.Remove(hydrant.GetPrimaryKeyLong());
                break;

            case ICheckpointGrain checkpoint:
                CheckpointIds.Remove(checkpoint.GetPrimaryKeyLong());
                break;

            default:
                throw new InvalidOperationException($"Unsupported unsubscription grain type: {grain.GetType().Name}");
        }
    }
}

[GenerateSerializer]
public sealed record DeviceStatusChanged
{
    [Id(0)] public required string DeviceId { get; set; }
    [Id(1)] public required string Status { get; set; }
    [Id(2)] public required DateTimeOffset LastUpdated { get; set; } 
}
