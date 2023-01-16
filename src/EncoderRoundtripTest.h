#pragma once
/**
 * @brief Takes a int64_t vector and measures the time it takes to encode and decode
 *        parquet format with DELTA_BINARY_PACKED encoding
 * 
 * @param sample_repeat The number of times to repeat the measurement to account for disturbances
 * @param in_data The data to use for the roundtrip
 * @return A std::pair of encoding time and decoding time as int64_t in µs
 */
std::pair<int64_t, int64_t> encoder_roundtrip(int sample_repeat, std::vector<int64_t> &in_data);