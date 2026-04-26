// C# gRPC server fixture for IDL cross-repo binding tests.
//
// Mirrors what Grpc.Tools emits via protoc-gen-grpc-csharp: a generated nested
// base class named <Service>.<Service>Base. User code derives from <Service>Base
// and overrides each rpc method with the *Async suffix.
//
// The cbm pass_idl_scan should detect:
//   - PromoCodeService Class node (from contracts/promo.proto)
//   - INHERITS edge from PromoCodeServiceImpl → PromoCodeServiceBase
//   - Strip the "Base" suffix to derive the service name
//   - Strip the "Async" suffix from method names before route lookup
//   - Emit HANDLES edges from each method to __route__grpc__PromoCodeService/<method>

namespace Promo.V1.Server;

// Stand-in for Grpc.Tools-generated PromoCodeServiceBase.
public abstract class PromoCodeServiceBase
{
    public virtual System.Threading.Tasks.Task<GetVoucherResponse> GetVoucherAsync(
        GetVoucherRequest request, Grpc.Core.ServerCallContext context)
        => throw new System.NotImplementedException();

    public virtual System.Threading.Tasks.Task<RedeemVoucherResponse> RedeemVoucherAsync(
        RedeemVoucherRequest request, Grpc.Core.ServerCallContext context)
        => throw new System.NotImplementedException();
}

public class PromoCodeServiceImpl : PromoCodeServiceBase
{
    private readonly IVoucherStore _store;

    public PromoCodeServiceImpl(IVoucherStore store)
    {
        _store = store;
    }

    public override System.Threading.Tasks.Task<GetVoucherResponse> GetVoucherAsync(
        GetVoucherRequest request, Grpc.Core.ServerCallContext context)
    {
        var voucher = _store.Get(request.VoucherId);
        return System.Threading.Tasks.Task.FromResult(voucher);
    }

    public override System.Threading.Tasks.Task<RedeemVoucherResponse> RedeemVoucherAsync(
        RedeemVoucherRequest request, Grpc.Core.ServerCallContext context)
    {
        var ok = _store.Redeem(request.VoucherId, request.UserId);
        return System.Threading.Tasks.Task.FromResult(ok);
    }
}

public interface IVoucherStore
{
    GetVoucherResponse Get(string voucherId);
    RedeemVoucherResponse Redeem(string voucherId, string userId);
}

public class GetVoucherRequest { public string VoucherId { get; set; } = ""; }
public class GetVoucherResponse { public string VoucherId { get; set; } = ""; public int DiscountPercent { get; set; } public bool IsValid { get; set; } }
public class RedeemVoucherRequest { public string VoucherId { get; set; } = ""; public string UserId { get; set; } = ""; }
public class RedeemVoucherResponse { public bool Success { get; set; } public string ErrorMessage { get; set; } = ""; }
