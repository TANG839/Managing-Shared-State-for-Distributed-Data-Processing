include(FetchContent)
FetchContent_Declare(
        azure-storage-lite
        GIT_REPOSITORY https://github.com/Azure/azure-storage-cpplite.git
        GIT_TAG 979c59ebe1c247c1126524953c31d75693110ce9
)
FetchContent_GetProperties(azure-storage-lite)
if (NOT azure-storage-lite_POPULATED)
    FetchContent_Populate(azure-storage-lite)
    add_subdirectory(${azure-storage-lite_SOURCE_DIR} ${azure-storage-lite_BINARY_DIR} EXCLUDE_FROM_ALL)
endif ()
target_compile_options(azure-storage-lite PRIVATE -w)
target_include_directories(azure-storage-lite SYSTEM INTERFACE ${azure-storage-lite_SOURCE_DIR}/include/)

