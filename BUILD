cc_library(
    name = "recordio",
    srcs = ["reader.cc"],
    hdrs = ["recordio.h"],
    linkopts = ["-lz"],
    visibility = ["//visibility:public"],
    deps = ["//lib/file"],
)

cc_test(
    name = "recordio_test",
    size = "small",
    srcs = ["recordio_test.cc"],
    data = ["//lib/recordio/testdata"],
    linkstatic = 1,
    deps = [
        ":recordio",
        "@com_github_google_googletest//:gmock",
        "@com_github_google_googletest//:gtest",
    ],
)
