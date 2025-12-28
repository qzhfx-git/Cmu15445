//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "buffer/arc_replacer.h"  // <--- ✅ 加回这一行！(为了 AccessType)
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id, page_id_t page_id = INVALID_PAGE_ID)
    : frame_id_(frame_id), page_id_(page_id), data_(BUSTUB_PAGE_SIZE, 0) {
  Reset();
}

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, 2)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  std::scoped_lock lock(*bpm_latch_);
  frame_id_t frame_id = -1;
  if (free_frames_.size() > 0) {
    frame_id = free_frames_.front();
    free_frames_.pop_front();
  } else {
    auto fid = replacer_->Evict();
    if (fid.has_value()) {
      frame_id = fid.value();
    } else {
      return -1;
    }
    auto frame_header = frames_[frame_id];
    if (frame_header->is_dirty_) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      DiskRequest write_req;
      write_req.is_write_ = true;
      write_req.data_ = frames_[frame_id]->GetDataMut();
      write_req.page_id_ = frame_header->page_id_;
      write_req.callback_ = std::move(promise);
      std::vector<DiskRequest> requests;
      requests.push_back(std::move(write_req));
      disk_scheduler_->Schedule(requests);
      future.get();
    }
    page_table_.erase(frame_header->page_id_);
  }
  page_id_t new_page_id = next_page_id_++;
  page_table_[new_page_id] = frame_id;
  auto header = frames_[frame_id];
  header->Reset();
  header->page_id_ = new_page_id;
  header->pin_count_ = 0;
  header->is_dirty_ = false;  // 新纸是干净的
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return new_page_id;
}

// 返回拿到该页面的 frame_id
frame_id_t BufferPoolManager::GetFrameForPage(page_id_t page_id) {
  // std::scoped_lock lock(*bpm_latch_);

  // 先查表：是否已经在内存里？
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];

    replacer_->RecordAccess(frame_id);
    // replacer_->SetEvictable(frame_id, false);
    frames_[frame_id]->pin_count_++;
    replacer_->SetEvictable(frame_id, false);
    return frame_id;
  }

  // 没在内存里 -> 需要从磁盘加载

  frame_id_t frame_id = -1;

  // A. 找空闲帧
  if (!free_frames_.empty()) {
    frame_id = free_frames_.front();
    free_frames_.pop_front();
  } else {
    auto victim_opt = replacer_->Evict();
    if (!victim_opt.has_value()) {
      return -1;  // 内存已满且不可驱逐
    }
    frame_id = victim_opt.value();

    // C. 写回脏页 (如果有)
    auto old_header = frames_[frame_id];
    if (old_header->is_dirty_) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      DiskRequest write_req;
      write_req.is_write_ = true;
      write_req.data_ = frames_[frame_id]->GetDataMut();
      write_req.page_id_ = old_header->page_id_;
      write_req.callback_ = std::move(promise);
      std::vector<DiskRequest> requests;
      requests.push_back(std::move(write_req));
      disk_scheduler_->Schedule(requests);
      future.get();
    }
    page_table_.erase(old_header->page_id_);
  }

  // 3. 从磁盘读取新数据 (这是 Fetch/Read/Write 特有的)
  auto header = frames_[frame_id];

  header->Reset();  // 清空内存
  // 重置头信息
  header->page_id_ = page_id;
  header->pin_count_ = 1;
  header->is_dirty_ = false;

  // 发起读请求
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();

  DiskRequest read_req;
  read_req.is_write_ = false;  // 读！
  read_req.data_ = header->GetDataMut();
  read_req.page_id_ = page_id;
  read_req.callback_ = std::move(promise);

  std::vector<DiskRequest> reqs;
  reqs.push_back(std::move(read_req));
  disk_scheduler_->Schedule(reqs);

  // 等待读取完成
  if (!future.get()) {
    // 理论上磁盘读不应该失败，但防一手
    // 如果失败可能需要回滚 pin_count 等
  }

  // 4. 更新元数据
  page_table_[page_id] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return frame_id;
}
/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places that a page or a page's metadata could be, and use that to guide you on implementing
 * this function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * You should call `DeallocatePage` in the disk scheduler to make the space available for new pages.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock lock(*bpm_latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    disk_scheduler_->DeallocatePage(page_id);
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  auto header = frames_[frame_id];

  // 如果还有人在用 (Pin Count > 0)，则不能删除
  if (header->pin_count_ > 0) {
    return false;
  }

  // A. 从页表中移除
  page_table_.erase(page_id);

  // B. 停止 LRU 跟踪
  replacer_->Remove(frame_id);

  // C. 重置 Frame 元数据
  header->page_id_ = INVALID_PAGE_ID;
  header->is_dirty_ = false;
  header->pin_count_ = 0;
  header->Reset();  // 清空内存数据

  // D. 归还到空闲链表 (这一步最重要，让坑位可被复用)
  free_frames_.push_back(frame_id);

  // 5. 在磁盘上释放
  disk_scheduler_->DeallocatePage(page_id);

  return true;
}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are three main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`; otherwise, returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  frame_id_t frame_id;
  {
    std::scoped_lock lock(*bpm_latch_);  // 必须上大锁

    frame_id = GetFrameForPage(page_id);  // 调用上面的辅助逻辑
  }
  if (frame_id == -1) {
    return std::nullopt;
  }

  // 构造 WritePageGuard (逻辑和 Read 一样，只是返回类型不同)
  return WritePageGuard(page_id, frames_[frame_id], replacer_, bpm_latch_, disk_scheduler_);
}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`; otherwise, returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  frame_id_t frame_id;
  {
    std::scoped_lock lock(*bpm_latch_);

    frame_id = GetFrameForPage(page_id);  // 调用上面的辅助逻辑
  }

  if (frame_id == -1) {
    return std::nullopt;
  }

  // 构造 ReadPageGuard
  // 参数：page_id, frame指针, replacer指针, latch指针, disk_scheduler指针
  return ReadPageGuard(page_id, frames_[frame_id], replacer_, bpm_latch_, disk_scheduler_);
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer
 * pool manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer
 * pool manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk unsafely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory,
 * this function will return `false`.
 *
 * You should not take a lock on the page in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table; otherwise, `true`.
 */
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool { UNIMPLEMENTED("TODO(P1): Add implementation."); }

/**
 * @brief Flushes a page's data out to disk safely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory,
 * this function will return `false`.
 *
 * You should take a lock on the page in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `Flush` in the page guards, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table; otherwise, `true`.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock lock(*bpm_latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  auto header = frames_[frame_id];

  // 发起写请求 (即使 is_dirty_ 为 false，FlushPage 语义通常也要求强制覆盖磁盘)

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();

  DiskRequest req;
  req.is_write_ = true;
  req.data_ = header->GetDataMut();
  req.page_id_ = page_id;
  req.callback_ = std::move(promise);

  std::vector<DiskRequest> reqs;
  reqs.push_back(std::move(req));
  disk_scheduler_->Schedule(reqs);

  // if (future.wait_for(std::chrono::seconds(1)) == std::future_status::timeout) {
  //   // 生产环境应该处理超时，这里仅作演示
  //   return false;
  // }
  future.get();  // 确保写完

  header->is_dirty_ = false;

  return true;
}

/**
 * @brief Flushes all page data that is in memory to disk unsafely.
 *
 * You should not take locks on the pages in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPagesUnsafe() { UNIMPLEMENTED("TODO(P1): Add implementation."); }

/**
 * @brief Flushes all page data that is in memory to disk safely.
 *
 * You should take locks on the pages in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPages() { UNIMPLEMENTED("TODO(P1): Add implementation."); }

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple
 * threads access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely
 * cause problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds
 * the page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will
 * still need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists; otherwise, `std::nullopt`.
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::scoped_lock lock(*bpm_latch_);

  //  查表，看页面是否在内存中
  auto it = page_table_.find(page_id);

  // 如果不在，返回 nullopt
  if (it == page_table_.end()) {
    return std::nullopt;
  }

  // 如果在，返回 frame 的 pin_count
  return frames_[it->second]->pin_count_.load();
}

}  // namespace bustub
