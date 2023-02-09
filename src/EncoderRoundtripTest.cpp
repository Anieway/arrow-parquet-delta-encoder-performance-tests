#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
#include <chrono>
#include <algorithm>
#include "parquet/schema.h"
#include "parquet/encoding.h"
#include "parquet/types.h"

#include "EncoderRoundtripTest.h"
/**
 * @brief Executes a set amout of parquet-encoder(DELTA_BINARY_PACKED encoding) roundtrips with the given data
 *        and returns the best measurement pair (sorted by encoding time) in µs
 * 
 * @param sample_repeat the number of 
 * @param in_data An int64_t vector containing the data to be used for the round trips
 * @return std::pair<int64_t, int64_t> a pair of (encode time, decode time) in µs
 */
std::pair<int64_t, int64_t> encoder_roundtrip(int sample_repeat, std::vector<int64_t> &in_data) {
    //test repeatedly and pick minimum result
    std::vector<std::pair<int64_t, int64_t>> measurements;
    for(int i=0; i<sample_repeat; ++i) {
        //________________start_test________________
        auto node = parquet::schema::Int64("Test", parquet::Repetition::REQUIRED);
        auto columnDescr = std::make_shared<parquet::ColumnDescriptor>(node, 0, 0);

        auto encoder =
            parquet::MakeTypedEncoder<parquet::Int64Type>(parquet::Encoding::DELTA_BINARY_PACKED, false, columnDescr.get());
        auto decoder = parquet::MakeTypedDecoder<parquet::Int64Type>(parquet::Encoding::DELTA_BINARY_PACKED, columnDescr.get());

        std::vector<int64_t> out_data;
        out_data.resize(in_data.size());
        //start timing encoding
        const auto startE = std::chrono::steady_clock::now();
        //encode
        encoder->Put(in_data.data(), in_data.size());
        auto encode_buffer = encoder->FlushValues();
        //stop timing encoding & start timeing decoding
        const auto mid = std::chrono::steady_clock::now();
        //decode
        decoder->SetData(in_data.size(), encode_buffer->data(),
                            static_cast<int>(encode_buffer->size()));
        int values_decoded = decoder->Decode(out_data.data(), out_data.size());
        //stop timing decoding
        const auto endD = std::chrono::steady_clock::now();
        //check output volume
        if(values_decoded != in_data.size()) {
            std::cerr << "Decoded " << values_decoded << " values but expected " << in_data.size() << " !\n";
        }
        //validate data
        int error_count{0};
        int err_output_limit{10};
        for(int i = 0; i < in_data.size(); ++i) {
            int64_t in{in_data.at(i)};
            int64_t out{out_data.at(i)};
            if(in != out) {
                ++error_count;
                if( err_output_limit < 0 || error_count <= err_output_limit)
                std::cerr << "Mismatching value #" << i << "was expected to be "
                            << in << " but was " << out << '\n';
                if(error_count == err_output_limit) {
                    std::cerr << "Too many mismatched values! Omitting output...\n";
                }
            }
        }
        if(error_count == 0) {
            //time encoding took in µs
            auto encMicroS =
            std::chrono::duration_cast<std::chrono::microseconds>(mid-startE).count();
            //time decoding took in µs
            auto decMicroS =
            std::chrono::duration_cast<std::chrono::microseconds>(endD-mid).count();
            //add this runs measurements to the list
            measurements.push_back(std::make_pair(encMicroS, decMicroS));
        }
        else {
            std::cerr << "Validation complete. Unsuccessful:\n"
                        << error_count << " missmatched values!\n";
        }
    }

    //calculate minimum
    std::sort(measurements.begin(), measurements.end(),
    [](std::pair<int64_t, int64_t> a, std::pair<int64_t, int64_t> b){return a.first < b.first;});
    return measurements.at(0);
}

/**
 * @brief Executes a set amout of parquet-encoder(DELTA_BINARY_PACKED encoding) roundtrips with the given data
 *        and returns the best measurement pair (sorted by encoding time) in µs
 * 
 * @param sample_repeat the number of 
 * @param in_data An int32_t vector containing the data to be used for the round trips
 * @return std::pair<int64_t, int64_t> a pair of (encode time, decode time) in µs
 */
std::pair<int64_t, int64_t> encoder_roundtrip(int sample_repeat, std::vector<int32_t> &in_data) {
    //test repeatedly and pick minimum result
    std::vector<std::pair<int64_t, int64_t>> measurements;
    for(int i=0; i<sample_repeat; ++i) {
        //________________start_test________________
        auto node = parquet::schema::Int32("Test", parquet::Repetition::REQUIRED);
        auto columnDescr = std::make_shared<parquet::ColumnDescriptor>(node, 0, 0);

        auto encoder =
            parquet::MakeTypedEncoder<parquet::Int32Type>(parquet::Encoding::DELTA_BINARY_PACKED, false, columnDescr.get());
        auto decoder = parquet::MakeTypedDecoder<parquet::Int32Type>(parquet::Encoding::DELTA_BINARY_PACKED, columnDescr.get());

        std::vector<int32_t> out_data;
        out_data.resize(in_data.size());
        //start timing encoding
        const auto startE = std::chrono::steady_clock::now();
        //encode
        encoder->Put(in_data.data(), in_data.size());
        auto encode_buffer = encoder->FlushValues();
        //stop timing encoding & start timeing decoding
        const auto mid = std::chrono::steady_clock::now();
        //decode
        decoder->SetData(in_data.size(), encode_buffer->data(),
                            static_cast<int>(encode_buffer->size()));
        int values_decoded = decoder->Decode(out_data.data(), out_data.size());
        //stop timing decoding
        const auto endD = std::chrono::steady_clock::now();
        //check output volume
        if(values_decoded != in_data.size()) {
            std::cerr << "Decoded " << values_decoded << " values but expected " << in_data.size() << " !\n";
        }
        //validate data
        int error_count{0};
        int err_output_limit{10};
        for(int i = 0; i < in_data.size(); ++i) {
            int32_t in{in_data.at(i)};
            int32_t out{out_data.at(i)};
            if(in != out) {
                ++error_count;
                if( err_output_limit < 0 || error_count <= err_output_limit)
                std::cerr << "Mismatching value #" << i << "was expected to be "
                            << in << " but was " << out << '\n';
                if(error_count == err_output_limit) {
                    std::cerr << "Too many mismatched values! Omitting output...\n";
                }
            }
        }
        if(error_count == 0) {
            //time encoding took in µs
            auto encMicroS =
            std::chrono::duration_cast<std::chrono::microseconds>(mid-startE).count();
            //time decoding took in µs
            auto decMicroS =
            std::chrono::duration_cast<std::chrono::microseconds>(endD-mid).count();
            //add this runs measurements to the list
            measurements.push_back(std::make_pair(encMicroS, decMicroS));
        }
        else {
            std::cerr << "Validation complete. Unsuccessful:\n"
                        << error_count << " missmatched values!\n";
        }
    }

    //calculate minimum
    std::sort(measurements.begin(), measurements.end(),
    [](std::pair<int64_t, int64_t> a, std::pair<int64_t, int64_t> b){return a.first < b.first;});
    return measurements.at(0);
}
