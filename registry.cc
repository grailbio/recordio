// This file implements a process-wide registry of transformer factories.  The
// registry is consulted by the Writer to build a transformer that compresses or
// encrypts blocks. The transformer names are written in the header block of
// every recordio file. The reader consults the registry to build a transformer
// that performs the reverse transformations.
#include <iostream>
#include <mutex>
#include <regex>
#include <unordered_map>

#include "lib/recordio/recordio.h"

namespace grail {
namespace recordio {
namespace {
using Callback = std::function<Error(const std::string& args,
                                     std::unique_ptr<Transformer>* tr)>;

// A transformer that returns the input as is.
class IdTransformerImpl : public Transformer {
 public:
  Error Transform(IoVec in, IoVec* out) {
    *out = in;
    return "";
  }
};

struct Entry {
  Callback transformer_factory;
  Callback untransformer_factory;
};

// Singleton registry
struct Registry {
  std::mutex mu;
  std::unordered_map<std::string, Entry> factories;
};

std::once_flag g_module_once;
Registry* g_registry;

void InitModule() { g_registry = new (Registry); }

// Given a config string, such as "flate 4", find the factory for "flate". *args
// is set to the part after the first space, ("4" in the example).
Error FindEntry(const std::string& config, const Entry** e, std::string* args) {
  std::call_once(g_module_once, InitModule);

  *e = nullptr;
  std::string name;
  *args = "";
  if (std::regex_match(config, std::regex("^(\\S+)$"))) {
    name = config;
  } else {
    std::smatch m;
    if (!std::regex_match(name, m, std::regex("^(\\S+)\\s+(.*)$"))) {
      std::ostringstream msg;
      msg << "Failed to extract transformer name from \"" << name << "\"";
      return msg.str();
    }
    name = m[1];
    *args = m[2];
  }
  std::lock_guard<std::mutex> l(g_registry->mu);
  auto it = g_registry->factories.find(name);
  if (it == g_registry->factories.end()) {
    std::ostringstream msg;
    msg << "Transformer " << name << " not found";
    return msg.str();
  }
  *e = &it->second;
  return "";
}
}  // namespace

void RegisterTransformer(const std::string& name,
                         const Callback& transformer_factory,
                         const Callback& untransformer_factory) {
  std::call_once(g_module_once, InitModule);
  std::lock_guard<std::mutex> l(g_registry->mu);
  auto it = g_registry->factories.find(name);
  if (it != g_registry->factories.end()) {
    std::cerr << "Transformer " << name << " registered twice";
    abort();
  }
  g_registry->factories[name] =
      Entry{transformer_factory, untransformer_factory};
}

Error GetTransformer(const std::vector<std::string>& names,
                     std::unique_ptr<Transformer>* tr) {
  tr->reset();
  if (names.size() == 0) {
    *tr = std::unique_ptr<Transformer>(new IdTransformerImpl);
    return "";
  }
  if (names.size() > 1) {
    return "Multiple transformers not supported yet";
  }

  const Entry* e;
  std::string args;
  Error err = FindEntry(names[0], &e, &args);
  if (err != "") return err;
  return e->transformer_factory(args, tr);
}

Error GetUntransformer(const std::vector<std::string>& names,
                       std::unique_ptr<Transformer>* tr) {
  tr->reset();
  if (names.size() == 0) {
    *tr = std::unique_ptr<Transformer>(new IdTransformerImpl);
    return "";
  }
  if (names.size() > 1) {
    return "Multiple untransformers not supported yet";
  }

  const Entry* e;
  std::string args;
  Error err = FindEntry(names[0], &e, &args);
  if (err != "") return err;
  return e->untransformer_factory(args, tr);
}

}  // namespace recordio
}  // namespace grail
