// Java gRPC client fixture for IDL cross-repo binding tests.
//
// Mirrors what protoc-gen-grpc-java emits: a stub factory `PromoCodeServiceGrpc`
// returns a typed `PromoCodeServiceBlockingStub`. RPC methods are invoked
// as lowerCamelCase methods on the stub.
//
// The cbm pass_idl_scan should detect:
//   - The `stub` variable typed as `PromoCodeServiceGrpc.PromoCodeServiceBlockingStub`
//     (suffix `BlockingStub`)
//   - Strip the `BlockingStub` suffix → service name `PromoCodeService`
//   - Match against the proto-derived `PromoCodeService` Class node
//   - For each `stub.<rpcName>(...)` call, capitalize the first character and
//     emit a `GRPC_CALLS` edge to the Route `__route__grpc__PromoCodeService/<RpcName>`.

package com.example.promo.client;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;

// Stand-in for protoc-gen-grpc-java-generated PromoCodeServiceGrpc.
class PromoCodeServiceGrpc {
    public static PromoCodeServiceBlockingStub newBlockingStub(Channel channel) {
        return new PromoCodeServiceBlockingStub();
    }

    public static class PromoCodeServiceBlockingStub {
        public GetVoucherResponse getVoucher(GetVoucherRequest req) { return new GetVoucherResponse(); }
        public RedeemVoucherResponse redeemVoucher(RedeemVoucherRequest req) { return new RedeemVoucherResponse(); }
    }
}

public class PromoCodeClient {
    private final PromoCodeServiceGrpc.PromoCodeServiceBlockingStub stub;

    public PromoCodeClient(String target) {
        Channel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        this.stub = PromoCodeServiceGrpc.newBlockingStub(channel);
    }

    public GetVoucherResponse fetchVoucher(String voucherId) {
        GetVoucherRequest request = new GetVoucherRequest();
        request.voucherId = voucherId;
        return stub.getVoucher(request);
    }

    public boolean redeem(String voucherId, String userId) {
        RedeemVoucherRequest req = new RedeemVoucherRequest();
        req.voucherId = voucherId;
        req.userId = userId;
        RedeemVoucherResponse resp = stub.redeemVoucher(req);
        return resp.success;
    }
}

class GetVoucherRequest { public String voucherId; }
class GetVoucherResponse {}
class RedeemVoucherRequest { public String voucherId; public String userId; }
class RedeemVoucherResponse { public boolean success; }
