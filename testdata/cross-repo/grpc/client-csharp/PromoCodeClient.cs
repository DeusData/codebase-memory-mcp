// C# gRPC client fixture for IDL cross-repo binding tests.
//
// Mirrors what Grpc.Tools emits via protoc-gen-grpc-csharp: a generated nested
// client class named `<Service>.<Service>Client` constructed with a Grpc.Net
// channel; rpcs are invoked as `*Async` methods on the client instance.
//
// The cbm pass_idl_scan should detect:
//   - The `_client` field typed as `PromoCodeService.PromoCodeServiceClient`
//     (suffix `Client`)
//   - Strip the `Client` suffix → service name `PromoCodeService`
//   - Match against the proto-derived `PromoCodeService` Class node
//   - For each `_client.<RpcName>Async(...)` call within `FetchVoucherAsync`,
//     strip the `Async` suffix and emit a `GRPC_CALLS` edge to the Route
//     `__route__grpc__PromoCodeService/<RpcName>`.

namespace Promo.V1.Client;

// Stand-in for Grpc.Tools-generated PromoCodeServiceClient.
public class PromoCodeServiceClient
{
    public PromoCodeServiceClient(Grpc.Net.Client.GrpcChannel channel) { _channel = channel; }
    private readonly Grpc.Net.Client.GrpcChannel _channel;

    public virtual System.Threading.Tasks.Task<GetVoucherResponse> GetVoucherAsync(
        GetVoucherRequest request, Grpc.Core.CallOptions options = default)
        => System.Threading.Tasks.Task.FromResult(new GetVoucherResponse());

    public virtual System.Threading.Tasks.Task<RedeemVoucherResponse> RedeemVoucherAsync(
        RedeemVoucherRequest request, Grpc.Core.CallOptions options = default)
        => System.Threading.Tasks.Task.FromResult(new RedeemVoucherResponse());
}

public class CheckoutFlow
{
    private readonly PromoCodeServiceClient _client;

    public CheckoutFlow(Grpc.Net.Client.GrpcChannel channel)
    {
        _client = new PromoCodeServiceClient(channel);
    }

    public async System.Threading.Tasks.Task<GetVoucherResponse> FetchVoucherAsync(string voucherId)
    {
        var request = new GetVoucherRequest { VoucherId = voucherId };
        return await _client.GetVoucherAsync(request);
    }

    public async System.Threading.Tasks.Task<bool> RedeemAsync(string voucherId, string userId)
    {
        var req = new RedeemVoucherRequest { VoucherId = voucherId, UserId = userId };
        var resp = await _client.RedeemVoucherAsync(req);
        return resp.Success;
    }
}

public class GetVoucherRequest { public string VoucherId { get; set; } = ""; }
public class GetVoucherResponse { public string VoucherId { get; set; } = ""; public int DiscountPercent { get; set; } public bool IsValid { get; set; } }
public class RedeemVoucherRequest { public string VoucherId { get; set; } = ""; public string UserId { get; set; } = ""; }
public class RedeemVoucherResponse { public bool Success { get; set; } public string ErrorMessage { get; set; } = ""; }
