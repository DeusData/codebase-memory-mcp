/* nomic-embed-code vector blob embedded via assembler.
 * Cross-platform: macOS (Mach-O) vs Linux (ELF) vs Windows (COFF). */

#if defined(__APPLE__)
    .section __DATA,__const
    .globl _PRETRAINED_VECTOR_BLOB
    .globl _PRETRAINED_VECTOR_BLOB_LEN
    .p2align 4
_PRETRAINED_VECTOR_BLOB:
    .incbin "vendored/nomic/code_vectors.bin"
_PRETRAINED_VECTOR_BLOB_END:

    .section __DATA,__const
    .p2align 2
_PRETRAINED_VECTOR_BLOB_LEN:
    .long _PRETRAINED_VECTOR_BLOB_END - _PRETRAINED_VECTOR_BLOB

#elif defined(_WIN32) || defined(__CYGWIN__) || defined(__MINGW32__)
    .section .rdata,"dr"
    .globl PRETRAINED_VECTOR_BLOB
    .globl PRETRAINED_VECTOR_BLOB_LEN
    .p2align 4
PRETRAINED_VECTOR_BLOB:
    .incbin "vendored/nomic/code_vectors.bin"
PRETRAINED_VECTOR_BLOB_END:

    .section .rdata,"dr"
    .p2align 2
PRETRAINED_VECTOR_BLOB_LEN:
    .long PRETRAINED_VECTOR_BLOB_END - PRETRAINED_VECTOR_BLOB

#else
    /* WHY: an ELF object that carries no .note.GNU-stack tells the linker
     * nothing about its stack requirement, and GNU ld then assumes the WORST
     * for the whole link — every Linux release binary shipped GNU_STACK RWE
     * because of this one omission. This is the only assembly source in the
     * build, so it alone decided that property. The note must stay even though
     * a blob of constant data obviously never executes: absence is the signal,
     * not the contents. -Wl,-z,noexecstack in the link flags enforces the
     * outcome, and scripts/ci/check-binary-composition.sh fails the release if
     * an executable stack ever comes back. */
    .section .note.GNU-stack,"",@progbits

    .section .rodata,"a",@progbits
    .globl PRETRAINED_VECTOR_BLOB
    .globl PRETRAINED_VECTOR_BLOB_LEN
    .p2align 4
PRETRAINED_VECTOR_BLOB:
    .incbin "vendored/nomic/code_vectors.bin"
PRETRAINED_VECTOR_BLOB_END:

    .section .rodata,"a",@progbits
    .p2align 2
PRETRAINED_VECTOR_BLOB_LEN:
    .long PRETRAINED_VECTOR_BLOB_END - PRETRAINED_VECTOR_BLOB
#endif
