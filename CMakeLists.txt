cmake_minimum_required(VERSION 3.16)

project(my_project
    VERSION 1.0
    LANGUAGES CXX
)

# Use modern C++
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Create executable
add_executable(my_app
    src/main.cpp
    
)

# Include headers
target_include_directories(my_app
    PRIVATE
        
)

# (Optional) warnings
if (MSVC)
    target_compile_options(my_app PRIVATE /W4)
else()
    target_compile_options(my_app PRIVATE -Wall -Wextra -Wpedantic)
endif()
