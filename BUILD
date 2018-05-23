cc_library(
    name = "recordio",
    srcs = [
        "chunk.cc",
        "chunk.h",
        "flate.cc",
        "header.cc",
        "header.h",
        "internal.cc",
        "internal.h",
        "legacy_reader.cc",
        "reader.cc",
        "registry.cc",
        "writer.cc",
    ],
    hdrs = [
        "recordio.h",
    ],
    copts = ["-fPIC"],
    linkopts = ["-lz"],
    visibility = ["//visibility:public"],
    deps = [
        "//lib/file",
        "@com_github_gist_panzi_portable_endian_h//:portable_endian",
    ],
)

cc_test(
    name = "recordio_test",
    size = "small",
    srcs = ["recordio_test.cc"],
    data = ["//lib/recordio/testdata"],
    linkstatic = 1,
    deps = [
        ":recordio",
        "//lib/test_util",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)
