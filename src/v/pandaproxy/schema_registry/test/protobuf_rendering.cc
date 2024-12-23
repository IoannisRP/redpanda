// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"
// #include "thirdparty/protobuf/descriptor.h"
// #include "thirdparty/protobuf/descriptor.pb.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

namespace {

struct simple_sharded_store {
    simple_sharded_store() {
        store.start(pps::is_mutable::yes, ss::default_smp_service_group())
          .get();
    }
    ~simple_sharded_store() { store.stop().get(); }
    simple_sharded_store(const simple_sharded_store&) = delete;
    simple_sharded_store(simple_sharded_store&&) = delete;
    simple_sharded_store& operator=(const simple_sharded_store&) = delete;
    simple_sharded_store& operator=(simple_sharded_store&&) = delete;

    pps::schema_id
    insert(const pps::canonical_schema& schema, pps::schema_version version) {
        const auto id = next_id++;
        store
          .upsert(
            pps::seq_marker{
              std::nullopt,
              std::nullopt,
              version,
              pps::seq_marker_key_type::schema},
            schema.share(),
            id,
            version,
            pps::is_deleted::no)
          .get();
        return id;
    }

    pps::schema_id next_id{1};
    pps::sharded_store store;
};

auto to_raw(std::string_view raw_proto, simple_sharded_store& s) {
    pps::canonical_schema cps = pps::make_canonical_protobuf_schema(
                  s.store,
                  pps::unparsed_schema{
                    pps::subject{"foo"},
                    pps::unparsed_schema_definition{
                      raw_proto, pps::schema_type::protobuf}})
                  .get();
    iobuf buf = pps::make_canonical_protobuf_schema(
                  s.store,
                  pps::unparsed_schema{
                    pps::subject{"foo"},
                    pps::unparsed_schema_definition{
                      raw_proto, pps::schema_type::protobuf}})
                  .get()
                  .def()
                  .raw()();
    iobuf_parser parser{std::move(buf)};
    return parser.read_string(parser.bytes_left());
}

auto make_proto_files() {
    //TODO(ik):
    // - ranges: add max keyword <- debug_string just replaces it with a number
    // - extention declarations + reserved declarations
    std::vector<std::string> protos;

    //proto 2
    {
        const std::string syntax = "syntax = \"proto2\";\n\n";
        protos.push_back(syntax);
    }
    {
        const std::string import = R"(syntax = "proto2";

import "google/protobuf/timestamp.proto";
import public "google/protobuf/any.proto";
import weak "google/protobuf/descriptor.proto";
)";
        protos.push_back(import);
    }
    {
        const std::string package = R"(syntax = "proto2";

package foo;

)";
        protos.push_back(package);
    }
    {
        const std::string top_level_options = R"(syntax = "proto2";

option java_package = "com.java.package";
option java_outer_classname = "TestProtos";
option optimize_for = SPEED;
option java_multiple_files = true;
option go_package = "path/to/go/package";
option cc_generic_services = false;
option java_generic_services = false;
option py_generic_services = false;
option deprecated = true;
option java_string_check_utf8 = true;
option cc_enable_arenas = false;
option objc_class_prefix = "objc_pref";
option csharp_namespace = "csharp.ns";
option swift_prefix = "swift_pref";
option php_class_prefix = "php_pref";
option php_namespace = "php\\ns";
option php_metadata_namespace = "php_meta_ns";
option ruby_package = "ruby::package";

message Empty {
}

)";
        protos.emplace_back(top_level_options);
    }
    {
        const std::string enumerator = R"(syntax = "proto2";

enum MyEnum {
  option allow_alias = true;
  option deprecated = false;
  VALUE_1 = -1 [deprecated = false];
  VALUE_2 = 2 [debug_redact = false];
  VALUE_3 = 9;
  VALUE_4 = 10;
  VALUE_5 = 10;
  reserved 3, 4 to 8;
  reserved "foo", "bar";
}

)";
        protos.emplace_back(enumerator);
    }
    {
        const std::string message = R"(syntax = "proto2";

enum OuterEnum {
  VALUE_1 = 0;
  VALUE_2 = 1;
}

message Outer {
  optional double d = 1;
  extensions 100 to 199;
}

message MyMessage {
  option message_set_wire_format = false;
  option no_standard_descriptor_accessor = false;
  option deprecated = false;
  message Inner {
    optional double inner_d = 1;
    extensions 100 to 199;
  }
  enum InnerEnum {
    IVALUE_1 = 1;
    IVALUE_2 = 2;
  }
  repeated double d = 1 [packed = true, deprecated = true];
  required float f = 2 [ctype = STRING];
  optional int32 i32 = 3 [jstype = JS_NORMAL];
  optional int64 i64 = 4 [lazy = false];
  optional uint32 ui32 = 5 [unverified_lazy = false];
  optional uint64 ui64 = 6 [weak = false];
  optional sint32 si32 = 7 [debug_redact = false];
  optional sint64 si64 = 8 [retention = RETENTION_UNKNOWN];
  optional fixed32 f32 = 9;
  optional fixed64 f64 = 20;
  optional sfixed32 sf32 = 21;
  optional sfixed64 sf64 = 22;
  optional bool bl = 23;
  optional string str = 24;
  optional bytes bs = 25;
  optional .Outer outer = 26;
  optional .OuterEnum e = 27;
  optional .MyMessage.Inner inner = 28;
  optional .MyMessage.InnerEnum ie = 29;
  oneof var {
    double var_d = 30;
    float var_f = 31;
  }
  map<string, uint32> map_field = 32;
  required group G = 33 {
    required string str_a = 1;
    optional string str_b = 2;
    repeated string str_c = 3;
  }
  extend .MyMessage.Inner {
    optional double inner_ext = 100;
  }
  reserved 11, 17 to 19;
  reserved "foo", "bar";
}

extend .Outer {
  optional double outer_ext = 100;
}

)";
        protos.emplace_back(message);
    }
    {
        const std::string service = R"(syntax = "proto2";

message Request {
}

message Response {
}

service MyService {
  option deprecated = false;
  rpc Rpc_a(.Request) returns (.Response) {
    option deprecated = true;
    option idempotency_level = IDEMPOTENCY_UNKNOWN;
  }
  rpc Rpc_b(stream .Request) returns (stream .Response);
}

)";
        protos.emplace_back(service);
    }
    {
        const std::string custom_options = R"(syntax = "proto2";

import "google/protobuf/descriptor.proto";
option (.my_file_option) = "Hello world!";

enum MyEnum {
  option (.my_enum_option) = true;
  FOO = 1 [(.my_enum_value_option) = 321];
  BAR = 2;
}

message MyMessage {
  option (.my_message_option) = 1234;
  optional int32 foo = 1 [(.my_field_option) = 4.5];
  optional string bar = 2;
  oneof qux {    option (.my_oneof_option) = 42;

    string quux = 3;
  }
}

message RequestType {
}

message ResponseType {
}

service MyService {
  option (.my_service_option) = FOO;
  rpc MyMethod(.RequestType) returns (.ResponseType) {
    option (.my_method_option) = {
      foo: 567
      bar: "Some string"
    };
  }
}

extend .google.protobuf.FileOptions {
  optional string my_file_option = 50000;
}

extend .google.protobuf.MessageOptions {
  optional int32 my_message_option = 50001;
}

extend .google.protobuf.FieldOptions {
  optional float my_field_option = 50002;
}

extend .google.protobuf.OneofOptions {
  optional int64 my_oneof_option = 50003;
}

extend .google.protobuf.EnumOptions {
  optional bool my_enum_option = 50004;
}

extend .google.protobuf.EnumValueOptions {
  optional uint32 my_enum_value_option = 50005;
}

extend .google.protobuf.ServiceOptions {
  optional .MyEnum my_service_option = 50006;
}

extend .google.protobuf.MethodOptions {
  optional .MyMessage my_method_option = 50007;
}

)";
        protos.emplace_back(custom_options );
    }
    //proto 3
    {
        const std::string syntax = "syntax = \"proto3\";\n\n";
        protos.push_back(syntax);
    }
    {
        const std::string import = R"(syntax = "proto3";

import "google/protobuf/timestamp.proto";
import public "google/protobuf/any.proto";
import weak "google/protobuf/descriptor.proto";
)";
        protos.push_back(import);
    }
    {
        const std::string package = R"(syntax = "proto3";

package foo;

)";
        protos.push_back(package);
    }
    {
        const std::string top_level_options = R"(syntax = "proto3";

option java_package = "com.java.package";
option java_outer_classname = "TestProtos";
option optimize_for = SPEED;
option java_multiple_files = true;
option go_package = "path/to/go/package";
option cc_generic_services = false;
option java_generic_services = false;
option py_generic_services = false;
option deprecated = true;
option java_string_check_utf8 = true;
option cc_enable_arenas = false;
option objc_class_prefix = "objc_pref";
option csharp_namespace = "csharp.ns";
option swift_prefix = "swift_pref";
option php_class_prefix = "php_pref";
option php_namespace = "php\\ns";
option php_metadata_namespace = "php_meta_ns";
option ruby_package = "ruby::package";

message Empty {
}

)";
        protos.emplace_back(top_level_options);
    }
    {
        const std::string enumerator = R"(syntax = "proto3";

enum MyEnum {
  option allow_alias = true;
  option deprecated = false;
  VALUE_1 = 0 [deprecated = false];
  VALUE_2 = 1 [debug_redact = false];
  VALUE_3 = -9;
  VALUE_4 = 10;
  VALUE_5 = 10;
  reserved 2, 3 to 8;
  reserved "foo", "bar";
}

)";
        protos.emplace_back(enumerator);
    }
    {
        const std::string message = R"(syntax = "proto3";

enum OuterEnum {
  VALUE_1 = 0;
  VALUE_2 = 1;
}

message Outer {
}

message MyMessage {
  option message_set_wire_format = false;
  option no_standard_descriptor_accessor = false;
  option deprecated = false;
  message Inner {
    double inner_d = 1;
  }
  enum InnerEnum {
    IVALUE_1 = 0;
    IVALUE_2 = 1;
  }
  repeated double d = 1 [packed = true, deprecated = true];
  optional float f = 2 [ctype = STRING];
  int32 i32 = 3 [jstype = JS_NORMAL];
  int64 i64 = 4 [lazy = false];
  uint32 ui32 = 5 [unverified_lazy = false];
  uint64 ui64 = 6 [weak = false];
  sint32 si32 = 7 [debug_redact = false];
  sint64 si64 = 8 [retention = RETENTION_UNKNOWN];
  fixed32 f32 = 9;
  fixed64 f64 = 20;
  sfixed32 sf32 = 21;
  sfixed64 sf64 = 22;
  bool bl = 23;
  string str = 24;
  bytes bs = 25;
  .Outer outer = 26;
  .OuterEnum e = 27;
  .MyMessage.Inner inner = 28;
  .MyMessage.InnerEnum ie = 29;
  oneof var {
    double var_d = 30;
    float var_f = 31;
  }
  map<string, uint32> map_field = 32;
  reserved 11, 17 to 19;
  reserved "foo", "bar";
}

)";
        protos.emplace_back(message);
    }
    {
        const std::string service = R"(syntax = "proto3";

message Request {
}

message Response {
}

service MyService {
  option deprecated = false;
  rpc Rpc_a(.Request) returns (.Response) {
    option deprecated = true;
    option idempotency_level = IDEMPOTENCY_UNKNOWN;
  }
  rpc Rpc_b(stream .Request) returns (stream .Response);
}

)";
        protos.emplace_back(service);
    }
    {
        const std::string custom_options = R"(syntax = "proto3";

import "google/protobuf/descriptor.proto";
option (.my_file_option) = "Hello world!";

enum MyEnum {
  option (.my_enum_option) = true;
  FOO = 0 [(.my_enum_value_option) = 321];
  BAR = 2;
}

message MyMessage {
  option (.my_message_option) = 1234;
  optional int32 foo = 1 [(.my_field_option) = 4.5];
  optional string bar = 2;
  oneof qux {    option (.my_oneof_option) = 42;

    string quux = 3;
  }
}

message RequestType {
}

message ResponseType {
}

service MyService {
  option (.my_service_option) = FOO;
  rpc MyMethod(.RequestType) returns (.ResponseType) {
    option (.my_method_option) = {
      foo: 567
      bar: "Some string"
    };
  }
}

extend .google.protobuf.FileOptions {
  optional string my_file_option = 50000;
}

extend .google.protobuf.MessageOptions {
  optional int32 my_message_option = 50001;
}

extend .google.protobuf.FieldOptions {
  optional float my_field_option = 50002;
}

extend .google.protobuf.OneofOptions {
  optional int64 my_oneof_option = 50003;
}

extend .google.protobuf.EnumOptions {
  optional bool my_enum_option = 50004;
}

extend .google.protobuf.EnumValueOptions {
  optional uint32 my_enum_value_option = 50005;
}

extend .google.protobuf.ServiceOptions {
  optional .MyEnum my_service_option = 50006;
}

extend .google.protobuf.MethodOptions {
  optional .MyMessage my_method_option = 50007;
}

)";
        protos.emplace_back(custom_options );
    }

    return protos;
}

} // namespace

TEST(protobuf_rendering, basic_testing) {
    simple_sharded_store s;
    for (const std::string& proto_file : make_proto_files()) {
        const auto raw = to_raw(proto_file, s);
        // std::cout<<"-- before::\n"<<message<<std::endl;
        // std::cout<<"-- after::\n"<<raw<<std::endl;

        EXPECT_EQ(proto_file, std::string{raw});
    }
}

// namespace gpb = google::protobuf;
////pseudo code...
// std::vector<std::string> proto_files();
// ss::future<const gpb::FileDescriptor*> compile(const std::string&);
// TEST(protobuf_rendering, test_rendering) {
//     for (const auto& proto_file : proto_files()) {
//         const auto* orig_fd = compile(proto_file).get();
//
//         const auto raw = to_raw(message);
//         const auto* rendered_fd = compile(raw).get();
//         EXPECT_EQ(*orig_fd, *rendered_fd);
//
//         const auto normalized_raw = to_raw(message, normalized::yes);
//         const auto* normalized_fd = compile(normalized_raw).get();
//         EXPECT_EQ(*orig_fd, *normalized_fd);
//     }
// }
