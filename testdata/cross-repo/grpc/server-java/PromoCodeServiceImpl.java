// Java gRPC server fixture for IDL cross-repo binding tests.
//
// Mirrors what protoc-gen-grpc-java emits: an inner abstract class named
// <Service>ImplBase that user code extends. cbm pass_idl_scan should detect:
//   - INHERITS edge from PromoCodeServiceImpl → PromoCodeServiceImplBase
//   - Strip "ImplBase" suffix to derive service name "PromoCodeService"
//   - Match each method to corresponding __route__grpc__PromoCodeService/<method>

package com.example.promo;

import io.grpc.stub.StreamObserver;

// Stand-in for protoc-gen-grpc-java-generated PromoCodeServiceGrpc.PromoCodeServiceImplBase.
abstract class PromoCodeServiceImplBase {
    public void GetVoucher(GetVoucherRequest request, StreamObserver<GetVoucherResponse> obs) {
        throw new UnsupportedOperationException();
    }

    public void RedeemVoucher(RedeemVoucherRequest request, StreamObserver<RedeemVoucherResponse> obs) {
        throw new UnsupportedOperationException();
    }
}

public class PromoCodeServiceImpl extends PromoCodeServiceImplBase {
    private final VoucherStore store;

    public PromoCodeServiceImpl(VoucherStore store) {
        this.store = store;
    }

    @Override
    public void GetVoucher(GetVoucherRequest request, StreamObserver<GetVoucherResponse> obs) {
        GetVoucherResponse v = store.get(request.getVoucherId());
        obs.onNext(v);
        obs.onCompleted();
    }

    @Override
    public void RedeemVoucher(RedeemVoucherRequest request, StreamObserver<RedeemVoucherResponse> obs) {
        RedeemVoucherResponse r = store.redeem(request.getVoucherId(), request.getUserId());
        obs.onNext(r);
        obs.onCompleted();
    }
}

interface VoucherStore {
    GetVoucherResponse get(String voucherId);
    RedeemVoucherResponse redeem(String voucherId, String userId);
}

class GetVoucherRequest { public String getVoucherId() { return ""; } }
class GetVoucherResponse {}
class RedeemVoucherRequest { public String getVoucherId() { return ""; } public String getUserId() { return ""; } }
class RedeemVoucherResponse {}
