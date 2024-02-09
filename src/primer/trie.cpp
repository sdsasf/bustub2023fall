#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");
  if (root_ == nullptr) {
    return nullptr;
  }
  std::shared_ptr<const TrieNode> temp = root_;
  auto i = key.begin();
  while (i != key.end()) {
    auto p = temp->children_.find(*i);
    if (p != temp->children_.end()) {
      temp = p->second;
      ++i;
    } else {
      break;
    }
  }
  if (i != key.end() || temp->is_value_node_ == 0) {
    return nullptr;
  }
  auto res = dynamic_cast<const TrieNodeWithValue<T> *>(temp.get());
  if (res == nullptr) {
    return nullptr;
  }
  return (res->value_).get();
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}
template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");
  std::shared_ptr<TrieNode> cur;
  std::shared_ptr<TrieNode> parent;
  std::shared_ptr<const TrieNode> node = root_;
  std::shared_ptr<const TrieNode> new_root;
  if (root_ == nullptr) {
    auto j = key.begin();
    cur = std::make_shared<TrieNode>();
    new_root = cur;
    parent = cur;
    while (j != std::prev(key.end())) {
      cur = std::make_shared<TrieNode>();
      parent->children_[*j] = cur;
      parent = cur;
      ++j;
    }
    cur->children_[*j] = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    return Trie(new_root);
  }
  cur = std::shared_ptr<TrieNode>(root_->Clone()), parent = cur;
  new_root = cur;
  if (key.empty()) {
    new_root = std::make_shared<TrieNodeWithValue<T>>(new_root->children_, std::make_shared<T>(std::move(value)));
    return Trie(new_root);
  }
  auto i = key.begin();
  while (i != key.end()) {
    parent = cur;
    auto p = node->children_.find(*i);
    if (p != node->children_.end()) {
      node = p->second;
      cur = std::shared_ptr<TrieNode>(node->Clone());
      parent->children_[*i] = cur;
      ++i;
    } else {
      break;
    }
  }
  if (i != key.end()) {
    while (i != std::prev(key.end())) {
      cur = std::make_shared<TrieNode>();
      parent->children_[*i] = cur;
      parent = cur;
      ++i;
    }
    cur->children_[*i] = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  } else {
    parent->children_[key.back()] =
        std::make_shared<TrieNodeWithValue<T>>(cur->children_, std::make_shared<T>(std::move(value)));
  }
  return Trie(new_root);
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}
auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  if (root_ == nullptr) {
    return Trie(nullptr);
  }
  std::vector<std::shared_ptr<TrieNode>> stack;
  std::shared_ptr<TrieNode> cur = std::shared_ptr<TrieNode>(root_->Clone());
  std::shared_ptr<const TrieNode> node = root_;
  std::shared_ptr<const TrieNode> new_root = cur;
  if (key.empty()) {
    new_root = std::make_shared<TrieNode>(new_root->children_);
    return Trie(new_root);
  }
  auto i = key.begin();
  while (i != key.end()) {
    stack.push_back(cur);
    auto p = node->children_.find(*i);
    if (p != node->children_.end()) {
      node = p->second;
      cur = std::shared_ptr<TrieNode>(node->Clone());
      stack.back()->children_[*i] = cur;
      ++i;
    }
  }
  auto j = key.rbegin();
  if (!cur->children_.empty()) {
    stack.back()->children_[*j] = std::make_shared<TrieNode>(cur->children_);
    return Trie(new_root);
  }
  stack.back()->children_.erase(*j);
  cur = stack.back();
  stack.pop_back();
  ++j;
  while (j != key.rend() && !(cur->is_value_node_) && cur->children_.empty()) {
    cur = stack.back();
    stack.pop_back();
    cur->children_.erase(*j);
    ++j;
    if (stack.empty()) {
      return Trie(nullptr);
    }
  }
  return Trie(new_root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
