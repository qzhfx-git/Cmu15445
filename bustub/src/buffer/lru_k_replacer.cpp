//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

/*
  void AddAccess(size_t timestamp);
  size_t GetBackDist(size_t current_timestamp);
  bool GetIsEvictable();
  void SetIsEvictable(bool flag);
  frame_id_t GetFid();
  void SetFid(frame_id_t fid);
*/
frame_id_t LRUKNode::GetFid() { return fid_; }
void LRUKNode::SetFid(frame_id_t fid) { fid_ = fid; }
bool LRUKNode::GetIsEvictable() { return is_evictable_; }
void LRUKNode::SetIsEvictable(bool flag) { is_evictable_ = flag; }
void LRUKNode::AddAccess(size_t timestamp) {
  history_.emplace_back(timestamp);
  if (history_.size() > k_) {
    history_.pop_front();
  }
}
size_t LRUKNode::GetBackDist(size_t current_timestamp) {
  if (history_.size() < k_) {
    return std::numeric_limits<size_t>::max();
  }
  return (current_timestamp - *history_.begin());
}
/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame is successfully evicted, or `std::nullopt` if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard lock(latch_);
  if (curr_size_ <= 0) {
    return std::nullopt;
  }
  frame_id_t ans = -1;
  bool is_have_tier1 = false;
  const size_t max_timestamp = std::numeric_limits<size_t>::max();
  size_t min_timestamp = std::numeric_limits<size_t>::max();
  for (auto &[fid, node] : node_store_) {
    if (!node.GetIsEvictable()) {
      continue;
    }
    auto itstamp = node.GetBackDist(current_timestamp_);
    if (itstamp == max_timestamp) {
      if (is_have_tier1 != 1) {
        is_have_tier1 = 1;
        min_timestamp = node.history_.back();
        ans = fid;
      } else {
        if (min_timestamp > node.history_.back()) {
          min_timestamp = node.history_.back();
          ans = fid;
        }
      }
    } else if (is_have_tier1 == 1) {
      continue;
    } else {
      if (min_timestamp > node.history_.front()) {
        ans = fid;
        min_timestamp = node.history_.front();
      }
    }
  }
  curr_size_--;
  node_store_.erase(ans);
  return ans;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception("frame_id is invalid");
  }
  std::lock_guard locd(latch_);
  current_timestamp_++;
  if (node_store_.find(frame_id) != node_store_.end()) {
    auto &it = node_store_.at(frame_id);
    it.AddAccess(current_timestamp_);
  } else {
    LRUKNode newNode(k_);
    newNode.SetFid(frame_id);
    newNode.AddAccess(current_timestamp_);
    node_store_[frame_id] = std::move(newNode);
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception("SetEvictable: frame_id invalid");
  }

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  LRUKNode &node = it->second;

  // 只有当状态真的不一样时，才去改计数器
  if (node.GetIsEvictable() && !set_evictable) {
    // 原来能踢(true)，现在不能踢(false) -> 可踢总数 -1
    curr_size_--;
  } else if (!node.GetIsEvictable() && set_evictable) {
    // 原来不能踢(false)，现在能踢(true) -> 可踢总数 +1
    curr_size_++;
  }

  node.SetIsEvictable(set_evictable);
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  // 1. 查找
  auto it = node_store_.find(frame_id);

  // 找不到直接返回
  if (it == node_store_.end()) {
    return;
  }

  LRUKNode &node = it->second;

  // 2. 检查是否可驱逐
  if (!node.GetIsEvictable()) {
    throw Exception("Remove: Can't remove an un-evictable frame!");
  }

  // 3. 执行删除
  node_store_.erase(frame_id);
  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {
  std::lock_guard lock(latch_);
  return curr_size_;
}

}  // namespace bustub
