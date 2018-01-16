cc_library(
    name = "recordio",
    srcs = [
        "reader.cc",
        "writer.cc",
    ],
    hdrs = [
        "recordio.h",
        "recordio_internal.h",
    ],
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
        "@com_github_google_googletest//:gmock",
        "@com_github_google_googletest//:gtest",
    ],
)
