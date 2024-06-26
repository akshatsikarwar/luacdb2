cmake_minimum_required(VERSION 3.16)
project(luacdb2)
set(CMAKE_BUILD_TYPE RelWithDebInfo)

add_executable(luacdb2 luacdb2.c)
add_compile_options(-Wall -Wextra -pedantic -Werror)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

set(ENV{PKG_CONFIG_PATH} /opt/bb/lib/pkgconfig)
find_package(PkgConfig REQUIRED)

pkg_check_modules(LUA REQUIRED IMPORTED_TARGET lua53)
pkg_check_modules(CDB2 REQUIRED IMPORTED_TARGET cdb2api)

target_include_directories(luacdb2 PRIVATE ${CDB2_INCLUDE_DIRS} ${LUA_INCLUDE_DIRS})
target_link_libraries(luacdb2 PRIVATE ${CDB2_LINK_LIBRARIES} ${LUA_LINK_LIBRARIES} Threads::Threads)
