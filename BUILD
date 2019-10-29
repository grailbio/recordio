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
    linkopts = ["-lz"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_gist_panzi_portable_endian_h//:portable_endian",
    ],
)

cc_test(
    name = "recordio_test",
    size = "small",
    srcs = ["recordio_test.cc"],
    data = ["testdata"],
    linkstatic = 1,
    deps = [
        ":recordio",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)
