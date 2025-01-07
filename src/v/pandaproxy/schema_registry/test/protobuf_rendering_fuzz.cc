// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// #include "pandaproxy/schema_registry/protobuf.h"
// #include "pandaproxy/schema_registry/sharded_store.h"
// #include "pandaproxy/schema_registry/types.h"

#include "bytes/iobuf_parser.h"
#include "bytes/streambuf.h"

#include <fmt/core.h>
#include <fmt/ostream.h>

#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string_view>

// -------------------------------------------------------
// This section is meant to mirror the existing rendering functions
//

enum class pbversion { proto2, proto3 };

template<>
class fmt::formatter<pbversion> {
public:
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template<typename Context>
    constexpr auto format(const pbversion pbv, Context& ctx) const {
        switch (pbv) {
        case pbversion::proto2:
            return fmt::format_to(ctx.out(), "proto2");
        case pbversion::proto3:
            return fmt::format_to(ctx.out(), "proto3");
        }
    }
};

// TODO: tellp doesn't work with iobuf_ostream. Use a different method to
// find if any data where written to the ostream
class maybe_newline_guard {
    std::ostream& _os;
    std::ostream::pos_type _pos;

public:
    explicit maybe_newline_guard(std::ostream& os)
      : _os{os}
      , _pos{_os.tellp()} {}

    maybe_newline_guard(const maybe_newline_guard&) = delete;
    maybe_newline_guard& operator=(const maybe_newline_guard&) = delete;
    maybe_newline_guard(maybe_newline_guard&&) = delete;
    maybe_newline_guard& operator=(maybe_newline_guard&&) = delete;

    ~maybe_newline_guard() {
        const auto current_pos = _os.tellp();
        if (current_pos != _pos) {
            fmt::print(_os, "\n");
        }
    }
};

class indentation {
    size_t& _indent;

public:
    explicit indentation(size_t& indent)
      : _indent{indent} {
        _indent += 2;
    }

    indentation(const indentation&) = delete;
    indentation& operator=(const indentation&) = delete;
    indentation(indentation&&) = delete;
    indentation& operator=(indentation&&) = delete;

    ~indentation() { _indent -= 2; }
};

void render_syntax(
  std::ostream& os, pbversion pbv, bool always_render = false) {
    maybe_newline_guard s{os};
    if (pbv == pbversion::proto3 || always_render) {
        fmt::print(os, "syntax = \"{}\";\n", pbv);
    }
}

void render_package(std::ostream& os, std::string_view package_name) {
    maybe_newline_guard s{os};
    fmt::print(os, "package {};\n", package_name);
}

// This does nothing... it is here just for demonstration purposes
// To-be-deleted...
struct Empty {};
struct EnumField {
    std::string ident;
    int index;
    // std::vector<EnumValueOption> options;
};
// struct Reserved {...};
// struct EnumOption {...};

void render_entry(std::ostream&, size_t, Empty) {
    // do nothing
}

void render_entry(std::ostream& os, size_t indent, const EnumField& ef) {
    fmt::print(os, "{:{}}{} = {}", "", indent, ef.ident, ef.index);
    // TODO: deal with options...
    fmt::print(os, ";\n");
}

class EnumRenderer {
public:
    using entry = std::variant<Empty, EnumField /*, EnumOption, Reserved*/>;

private:
    std::string_view _name;
    std::vector<entry> _enum_body;

public:
    explicit EnumRenderer(std::string_view name)
      : _name(name) {}

    void add_entry(entry e) { _enum_body.push_back(std::move(e)); }

    void render(std::ostream& os, size_t indent) {
        fmt::print(os, "{:{}}enum {} {{\n", "", indent, _name);
        {
            indentation tab{indent};
            for (const entry& e : _enum_body) {
                std::visit(
                  [&](const auto& value) { render_entry(os, indent, value); },
                  e); // 2
            }
        }
        fmt::print(os, "{:{}}}}\n", "", indent);
    }
};

void render_enum(std::ostream& os, std::string_view package_name) {
    maybe_newline_guard s{os};
    fmt::print(os, "package {};\n", package_name);
}

// -------------------------------------------------------
// From here onwards is the fuzz code
//

// If this exception is encountered, it means
// there is an issue with the test...
struct test_error : std::runtime_error {
    explicit test_error(const std::string& msg)
      : std::runtime_error(msg) {}
};

// An integer range [_min, _min + _size).
class range {
public:
    using value_type = int;

private:
    int _min;
    int _size;

public:
    // Creates a range [0, max)
    // precondetion max > 0
    explicit range(int max)
      : _min{0}
      , _size{max} {}

    // Creates a range [min, max)
    // precondetion min >= 0
    // precondetion max > min
    explicit range(int min, int max)
      : _min{0}
      , _size{max - min} {}

    // Maps an integer from [-128, 128) to [_min, _min + _size)
    int map(char input) { return input % _size + _min; }
};

class binary_range {
public:
    using value_type = bool;

private:
    range _rng;

public:
    binary_range()
      : _rng{2} {}

    bool map(char input) {
        const auto rv = _rng.map(input);
        return static_cast<bool>(rv);
    }
};

template<typename EnumType>
class enum_range {
public:
    using value_type = EnumType;

private:
    range _rng;

public:
    enum_range()
      : _rng{static_cast<int>(EnumType::n_values)} {}

    EnumType map(char input) {
        const auto rv = _rng.map(input);
        return static_cast<EnumType>(rv);
    }
};

struct interrupt_t {};

// This range has a chance to return interrupt_t instead of
// a value in the Range, with a chance of 1/_interrupt
template<typename Range>
class interrupt_range {
public:
    using value_type = typename Range::value_type;

private:
    Range _rng;
    // chance to interrupt
    range _interrupt;

public:
    interrupt_range(Range rng, int interrupt)
      : _rng{rng}
      , _interrupt{interrupt} {}

    std::variant<value_type, interrupt_t> map(char input) {
        const auto interrupt_token = _interrupt.map(input);
        if (interrupt_token == 0) {
            return interrupt_t{};
        }
        return _rng.map(input);
    }
};

class data_stream {
    std::string_view _data;
    size_t _index{};
    bool _wrapped{false};

public:
    explicit data_stream(std::string_view data)
      : _data{data} {}

    char read() {
        const char res = _data[_index];
        ++_index;
        if (_index == _data.size()) {
            // wrap around
            _index = 0;
            _wrapped = true;
        }
        return res;
    }

    bool wrapped() { return _wrapped; }
};

pbversion generate_version(data_stream& ds) {
    const char byte = ds.read();
    const auto r = range{2}.map(byte);
    switch (r) {
    case 0:
        return pbversion::proto2;
    case 1:
        return pbversion::proto3;
    default:
        throw test_error{fmt::format("generate_version. r = {}", r)};
    }
}

class random_proto_file {
    iobuf_ostream _iob_os{};
    size_t _indent{};

    enum class top_level_commands {
        // import,
        // option,
        // message,
        enumerator,
        // extend,
        // service,
        n_values
    };

    enum class enum_body {
        // option,
        enum_field,
        // reserved,
        n_values
    };

public:
    explicit random_proto_file(data_stream& data) {
        generate_protobuf(_iob_os.ostream(), data);
    }

    std::string serialize() && {
        iobuf_parser parser{std::move(_iob_os).buf()};
        return parser.read_string(parser.bytes_left());
    }

private:
    static std::string generate_package_name(data_stream& /*ds*/) {
        // TODO:
        // make this random
        return "foo.bar";
    }

    static std::string generate_enum_name(data_stream& /*ds*/) {
        // TODO:
        // make this random and remove static values
        static size_t index = 0;
        return "MyEnum" + std::to_string(++index);
    }

    static std::string generate_enum_field_name(data_stream& /*ds*/) {
        // TODO:
        // make this random and remove static values
        static size_t index = 0;
        return "VALUE_" + std::to_string(++index);
    }

    static void
    write_version(std::ostream& os, data_stream& ds, pbversion pbv) {
        const bool always_render = [&]() {
            const char byte = ds.read();
            return binary_range{}.map(byte);
        }();

        render_syntax(os, pbv, always_render);
    }

    static void write_package(std::ostream& os, data_stream& ds) {
        const bool should_write = [&]() {
            const char byte = ds.read();
            return binary_range{}.map(byte);
        }();

        if (!should_write) {
            return;
        }

        const auto package_name = generate_package_name(ds);
        render_package(os, package_name);
    }

    void
    write_enumerator(std::ostream& os, data_stream& ds, pbversion /*pbv*/) {
        const std::string enum_name = generate_enum_name(ds);
        // TODO: validate that this name has not been used before
        // TODO: add this name to available types
        EnumRenderer er{enum_name};

        constexpr int expected_entries = 5;
        interrupt_range ieb{enum_range<enum_body>{}, expected_entries};

        // Generate enum body
        const auto get_token = [&]() {
            const char byte = ds.read();
            return ieb.map(byte);
        };

        int index = 0;
        auto token = get_token();
        // TODO: encapsulate this check
        while (token.index() == 0 && !ds.wrapped()) {
            enum_body eb = std::get<0>(token);
            switch (eb) {
            case enum_body::enum_field:
                er.add_entry(EnumField{generate_enum_field_name(ds), ++index});
                break;
            case enum_body::n_values:
                throw test_error{fmt::format("enum_body. eb = n_values")};
            }

            token = get_token();
        }

        er.render(os, _indent);
    }

    void generate_protobuf(std::ostream& os, data_stream& data) {
        const auto pbv = generate_version(data);
        fmt::print(std::cout, "pbv: {}\n", pbv);

        write_version(os, data, pbv);
        write_package(os, data);

        while (!data.wrapped()) {
            const auto top_level = [&]() {
                const char byte = data.read();
                return enum_range<top_level_commands>{}.map(byte);
            }();
            switch (top_level) {
            case top_level_commands::enumerator:
                write_enumerator(os, data, pbv);
                break;
            case top_level_commands::n_values:
                throw test_error{fmt::format("top_level. r = n_values")};
            }
        }
    }
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0) {
        return 0;
    }

    std::cout << "Generating protobuf..." << std::endl;
    // NOLINTNEXTLINE
    std::string_view d(reinterpret_cast<const char*>(data), size);
    data_stream ds{d};

    random_proto_file pf{ds};
    std::cout << "Generated proto:\n---------" << std::endl;
    std::cout << std::move(pf).serialize();
    std::cout << "---------" << std::endl;
    ;

    return 0;
}

// This is for development testing... To-be-deleted...
int main() {
    try {
        // NOLINTNEXTLINE
        const std::string seeds[] = {
          //"a0e",
          //"a1f",
          //"b0e",
          //"b1f",
          "a0ea01234567890",
          "asdffawiyefawww",
          "ThisisALongerMessage...Let'sSeeWhatItDoes",
        };
        for (const auto& seed : seeds) {
            std::cout << "\n\nSeed: " << seed << std::endl;
            // NOLINTNEXTLINE
            LLVMFuzzerTestOneInput(
              reinterpret_cast<const uint8_t*>(seed.data()), seed.size());
        }
    } catch (const std::exception& e) {
        std::cout << "Exception: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
