// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_ZSTDCOMPRESSOR_H
#define CEPH_ZSTDCOMPRESSOR_H

#define ZSTD_STATIC_LINKING_ONLY
#include "zstd/lib/zstd.h"

#include <memory>
#include "include/buffer.h"
#include "include/encoding.h"
#include "compressor/Compressor.h"

class ZstdCompressor : public Compressor {
 public:
  ZstdCompressor(CephContext *cct) : Compressor(COMP_ALG_ZSTD, "zstd"), cct(cct) {}

  int compress(const ceph::buffer::list &src, ceph::buffer::list &dst, std::optional<int32_t> &compressor_message) override {
    ZSTD_CStream *s = ZSTD_createCStream();
    if (!s) {
      return -ENOMEM;
    }
    // RAII wrapper so every error path frees the stream (no manual frees).
    // s is guaranteed non-null here, so the deleter never runs on NULL.
    std::unique_ptr<ZSTD_CStream, decltype(&ZSTD_freeCStream)> s_guard(
      s, &ZSTD_freeCStream);

    size_t const res = ZSTD_initCStream_srcSize(s, cct->_conf->compressor_zstd_level, src.length());
    if (ZSTD_isError(res)) {
      return -EINVAL;
    }
    auto p = src.begin();
    size_t left = src.length();

    size_t const out_max = ZSTD_compressBound(left);
    ceph::buffer::ptr outptr = ceph::buffer::create_small_page_aligned(out_max);
    ZSTD_outBuffer_s outbuf;
    outbuf.dst = outptr.c_str();
    outbuf.size = outptr.length();
    outbuf.pos = 0;

    while (left) {
      ceph_assert(!p.end());
      struct ZSTD_inBuffer_s inbuf;
      inbuf.pos = 0;
      inbuf.size = p.get_ptr_and_advance(left, (const char**)&inbuf.src);
      left -= inbuf.size;
      ZSTD_EndDirective const zed = (left==0) ? ZSTD_e_end : ZSTD_e_continue;
      size_t r = ZSTD_compressStream2(s, &outbuf, &inbuf, zed);
      if (ZSTD_isError(r)) {
	      return -EINVAL;
      }
    }
    ceph_assert(p.end());

    // prefix with decompressed length
    ceph::encode((uint32_t)src.length(), dst);
    dst.append(outptr, 0, outbuf.pos);
    return 0;
  }

  int decompress(const ceph::buffer::list &src, ceph::buffer::list &dst, std::optional<int32_t> compressor_message) override {
    auto i = std::cbegin(src);
    return decompress(i, src.length(), dst, compressor_message);
  }

  int decompress(ceph::buffer::list::const_iterator &p,
		 size_t compressed_len,
		 ceph::buffer::list &dst,
		 std::optional<int32_t> compressor_message) override {
    if (compressed_len < 4) {
      return -EINVAL;
    }
    compressed_len -= 4;
    uint32_t dst_len;
    ceph::decode(dst_len, p);

    ceph::buffer::ptr dstptr(dst_len);
    ZSTD_outBuffer_s outbuf;
    outbuf.dst = dstptr.c_str();
    outbuf.size = dstptr.length();
    outbuf.pos = 0;

    ZSTD_DStream *s = ZSTD_createDStream();
    if (!s) {
      return -ENOMEM;
    }
    // RAII wrapper so every error path (including the early p.end() return and
    // the new error checks below) frees the DStream. s is non-null here, so
    // the deleter never runs on NULL.
    std::unique_ptr<ZSTD_DStream, decltype(&ZSTD_freeDStream)> s_guard(
      s, &ZSTD_freeDStream);

    size_t const init_res = ZSTD_initDStream(s);
    if (ZSTD_isError(init_res)) {
      return -EINVAL;
    }

    // Tracks the most recent ZSTD_decompressStream return value; 0 means the
    // frame completed cleanly.
    size_t r = 0;
    while (compressed_len > 0) {
      if (p.end()) {
        // Truncated input: compressed_len claims more data than the buffer
        // actually contains.
        return -EINVAL;
      }
      ZSTD_inBuffer_s inbuf;
      inbuf.pos = 0;
      inbuf.size = p.get_ptr_and_advance(compressed_len,
					 (const char**)&inbuf.src);
      compressed_len -= inbuf.size;

      // Drive zstd until it has consumed this whole input chunk. zstd may stop
      // consuming input before the chunk is exhausted (e.g. if the output
      // buffer fills because the encoded dst_len prefix understated the real
      // size); looping on inbuf.pos ensures we don't silently drop the
      // remaining bytes of the chunk.
      while (inbuf.pos < inbuf.size) {
        size_t const prev_in_pos = inbuf.pos;
        size_t const prev_out_pos = outbuf.pos;
        r = ZSTD_decompressStream(s, &outbuf, &inbuf);
        if (ZSTD_isError(r)) {
          // Corrupt input, etc.
          return -EINVAL;
        }
        if (r == 0) {
          // Frame ended.
          break;
        }
        if (inbuf.pos == prev_in_pos && outbuf.pos == prev_out_pos) {
          // No forward progress on either input or output, yet input remains
          // and the frame has not ended. This happens when the output buffer
          // is full but zstd still has more to emit, i.e. dst_len understated
          // the real decompressed size. Treat as corrupt rather than silently
          // truncating (and avoid spinning forever). Note: zstd legitimately
          // consumes input without producing output (and vice versa), so we
          // only bail when *neither* advances.
          return -EINVAL;
        }
      }
    }

    // Verify the frame actually completed and produced exactly the advertised
    // number of bytes. r != 0 means zstd is still expecting more input (a
    // truncated frame); a short output means dst_len overstated the size.
    if (r != 0) {
      return -EINVAL;
    }
    if (outbuf.pos != dst_len) {
      return -EINVAL;
    }

    dst.append(dstptr, 0, outbuf.pos);
    return 0;
  }
 private:
  CephContext *const cct;
};

#endif
