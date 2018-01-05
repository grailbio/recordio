cc_library(
    name = "recordio",
    srcs = ["reader.cc"],
    hdrs = ["recordio.h"],
    linkopts = ["-lz"],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "recordio_test",
    size = "small",
    srcs = ["recordio_test.cc"],
    data = ["//lib/recordio/testdata"],
    deps = [
        ":recordio",
        "@com_github_google_googletest//:gtest",
    ],
)
