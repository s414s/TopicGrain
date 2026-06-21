namespace WebApplication3.Devices;

// this can be integrated into the main grain
public class Topic : Grain, ITopicGrain
{
    private DeviceState? _lastState;

    public List<long> SectorIds { get; set; } = [];
    public List<long> HydrantIds { get; set; } = [];
    public List<long> CheckpointIds { get; set; } = [];

    public List<GrainId> Subscribers { get; set; } = [];

    public Task Publish(DeviceState reading)
    {
        _lastState = reading;

        //GetGrain<INotificationSubscriberGrain>(5) using just a long key(not a GrainId) is ambiguous, and Orleans will throw an exception if more than one grain class implements that interface.
        foreach (var grainId in Subscribers)
        {
            Console.WriteLine($"Publishing for {grainId}");
            var grainTypeName = grainId.Type.ToString();

            var grain = GrainFactory.GetGrain<INotificationSubscriberGrain>(grainId); // full grainId needed in order to resolve class instance
            grain.ReceiveNotification(reading);
            //var grain = GrainFactory.GetGrain(grainId);

            //grain.GetType().GetMethod("ReceiveNotification")?.Invoke(grain, new object[] { reading });
            //var grainType = grain.GetType();

            //grain.ReceiveNotification(_lastState ?? new());

            //switch (grainId.Type.ToString())
            //{
            //    case "device":
            //        {
            //            var grain = GrainFactory.GetGrain<IDeviceGrain>(grainId);
            //            tasks.Add(grain.ReceiveNotification(reading));
            //            break;
            //        }

            //    case "checkpoint":
            //        {
            //            var grain = GrainFactory.GetGrain<ICheckpointGrain>(grainId);
            //            tasks.Add(grain.ReceiveNotification(reading));
            //            break;
            //        }

            //    default:
            //        Console.Error.WriteLine($"Unknown grain type '{grainId.Type}' in Subscribers — skipping.");
            //        break;
            //}
        }

        return Task.CompletedTask;
    }

    // Another Alternative would be
    //public Task Subscribe(GrainId grainId)

    public Task Subscribe(IAddressable grain)
    {
        Console.WriteLine($"subscribing {grain.GetPrimaryKey()}");

        // alternative
        var grainId = grain.GetGrainId();
        var grainKey = grainId.Key;
        var grainType = grainId.Type;

        Console.WriteLine($"subscribing {grainId}");
        if(!Subscribers.Contains(grainId))
            Subscribers.Add(grainId);

        Console.WriteLine($"total subscribers: {string.Join(", ", Subscribers.Select(x => x.ToString()))}");

        return Task.CompletedTask;
    }

    public Task Unsubscribe(IAddressable grain)
    {
        throw new NotImplementedException();
    }
}

public interface ITopicGrain : IGrainWithStringKey
{
    Task Subscribe(IAddressable grain);
    Task Unsubscribe(IAddressable grain);
    Task Publish(DeviceState reading);
}
