#pragma once
/**
 * @brief Takes a int64_t vector and measures the time it takes to encode and decode
 *        parquet format with DELTA_BINARY_PACKED encoding
 * 
 * @param sample_repeat The number of times to repeat the measurement to account for disturbances
 * @param in_data The int64_t data to use for the roundtrip
 * @return A std::pair of encoding time and decoding time as int64_t in µs
 */
std::pair<int64_t, int64_t> encoder_roundtrip(int sample_repeat, std::vector<int64_t> &in_data);

/**
 * @brief Takes a int32_t vector and measures the time it takes to encode and decode
 *        parquet format with DELTA_BINARY_PACKED encoding
 * 
 * @param sample_repeat The number of times to repeat the measurement to account for disturbances
 * @param in_data The int32_t data to use for the roundtrip
 * @return A std::pair of encoding time and decoding time as int64_t in µs
 */
std::pair<int64_t, int64_t> encoder_roundtrip(int sample_repeat, std::vector<int32_t> &in_data);

/**
 * @brief Executes a set amout of parquet-encoder(DELTA_BINARY_PACKED encoding) roundtrips with the given data
 *        and returns the best measurement pair (sorted by encoding time) in µs
 * 
 * @param sample_repeat the number of 
 * @param in_data An int64_t vector containing the data to be used for the round trips
 * @return std::pair<int64_t, int64_t> a pair of (encode time, decode time) in µs
 */
std::pair<int64_t, int64_t> encoder_detailed_roundtrip(int sample_repeat, std::vector<int64_t> &in_data)
