//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// trie.cpp
//
// Identification: src/primer/trie.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/trie.h"
#include <string_view>
#include <typeinfo>
#include "common/exception.h"
namespace bustub {

/**
 * @brief Get the value associated with the given key.
 * 1. If the key is not in the trie, return nullptr.
 * 2. If the key is in the trie but the type is mismatched, return nullptr.
 * 3. Otherwise, return the value.
 */
template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");
  if (root_ == nullptr) {
    return nullptr;
  }
  const TrieNode *ptr = root_.get();
  for (char c : key) {
    auto it = ptr->children_.find(c);

    if (it == ptr->children_.end()) {
      return nullptr;
    }
    ptr = it->second.get();
  }

  // const auto *value_node = std::dynamic_pointer_cast;
  const auto *value_node = dynamic_cast<const TrieNodeWithValue<T> *>(ptr);

  if (value_node != nullptr) {
    return value_node->value_.get();
  }
  return nullptr;
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

/**
 * @brief Put a new key-value pair into the trie. If the key already exists, overwrite the value.
 * @return the new trie.
 */
template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // 定义递归 Lambda
  // 返回值必须是 shared_ptr<const TrieNode>，这是 Trie 结构要求的
  std::function<std::shared_ptr<const TrieNode>(std::shared_ptr<const TrieNode>, size_t)> dfs;

  dfs = [&](std::shared_ptr<const TrieNode> curr, size_t idx) -> std::shared_ptr<const TrieNode> {
    std::shared_ptr<TrieNode> new_node_ptr;

    if (curr != nullptr) {
      // 如果当前节点存在，克隆它的 children map
      new_node_ptr = curr->Clone();
    } else {
      // 如果当前节点是空的（比如插入路径上原本不存在节点），新建一个空的
      new_node_ptr = std::make_shared<TrieNode>();
    }

    if (idx == key.size()) {
      return std::make_shared<TrieNodeWithValue<T>>(new_node_ptr->children_, std::make_shared<T>(std::move(value)));
    }

    char c = key[idx];

    std::shared_ptr<const TrieNode> child_node = nullptr;
    if (curr && curr->children_.count(c)) {
      child_node = curr->children_.at(c);
    }

    // 递归获取“新的子节点”
    auto new_child = dfs(child_node, idx + 1);
    new_node_ptr->children_[c] = new_child;

    return new_node_ptr;
  };

  // --- 启动递归 ---
  // 注意：如果 root_ 为空，我们传 nullptr 进去
  auto new_root = dfs(root_, 0);

  // 返回一个新的 Trie 对象，包含新的 root
  return Trie(new_root);
}

/**
 * @brief Remove the key from the trie.
 * @return If the key does not exist, return the original trie. Otherwise, returns the new trie.
 */
auto Trie::Remove(std::string_view key) const -> Trie {
  if (root_ == nullptr) {  // 空树
    return *this;
  }
  std::function<std::shared_ptr<const TrieNode>(std::shared_ptr<const TrieNode>, size_t)> dfs;

  dfs = [&](std::shared_ptr<const TrieNode> curr, size_t idx) -> std::shared_ptr<const TrieNode> {
    if (idx < key.size()) {  // 不是对应节点
      char c = key[idx];

      if (curr->children_.find(c) == curr->children_.end()) {  // 对应结点不存在
        return curr;
      }

      auto child = curr->children_.at(c);
      auto new_child = dfs(child, idx + 1);

      auto new_node = curr->Clone();  // 要改 children_

      if (new_child == nullptr) {
        // 子节点被完全删除了，我们也从 map 里删掉它
        new_node->children_.erase(c);
      } else {
        // 子节点还在（或者是新的副本），更新 map
        new_node->children_[c] = new_child;
      }

      if (!new_node->is_value_node_ && new_node->children_.empty()) {
        return nullptr;
      }

      return new_node;
    }

    // 当前结点不是值结点
    if (!curr->is_value_node_) {
      return curr;
    }

    auto new_node = std::make_shared<TrieNode>(curr->children_);

    // 它现在既没值也没孩子，直接删除
    if (new_node->children_.empty()) {
      return nullptr;
    }

    // 变成了普通节点的 node
    return new_node;
  };
  auto new_root = dfs(root_, 0);

  return Trie(new_root);
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value anymore,
  // you should convert it to `TrieNode`. If a node doesn't have children anymore, you should remove it.
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
