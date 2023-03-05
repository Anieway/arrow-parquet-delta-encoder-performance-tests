#include <iostream>
#include <fstream>
#include <stdexcept>
#include <vector>
#include <chrono>
#include "parquet/schema.h"
#include "parquet/encoding.h"
#include "parquet/types.h"

int main(int argc, char *argv[]) {
    if(argc != 2) {
        std::cerr << "Invalid Number of Arguments! Usage: " << argv[0]
                    << " <Number of Values to write>\n";
        return 1;
    }
    //______________Parsing_arguments___________
    //argument parsing adapted from here https://stackoverflow.com/a/2797823
    int64_t value_count; //value count
    //deltas to test (fitting within different bitwidths)
    std::array<int64_t, 11>deltas{{1, 10, 100, 500, 1000, 10000, 32000, 42000,
                                     1000000000, 3000000000, 4000000000000000000}};
    std::istringstream s1(argv[1]);
    if (!(s1 >> value_count)) {
        std::cerr << "Invalid number: " << argv[1] << '\n';
    } else if (!s1.eof()) {
        std::cerr << "Trailing characters after number: " << argv[1] << '\n';
    }
    //_______________Parsing_done_______________
    std::vector<int64_t> encodeResult;
    std::vector<int64_t> decodeResult;
    for(int64_t delta : deltas) {
        //fill some_data with values
        std::vector<int64_t> in_data;
        in_data.resize(value_count);
        //initialize data with evenly spaced values
        int64_t val = 0; //value that will progressively update beetween minvalue and maxvalue
        for(auto &elem : in_data) {
            elem = val;
            //overflow would result in out-of-spec delta for this test so make sure to keep limits
            if(val >= std::numeric_limits<int64_t>::max()-delta || val <= std::numeric_limits<int64_t>::min()+delta) { 
            delta = -delta;
            }
            val+=delta;
        }

        //calculate throughput
        float dataInMb = static_cast<float>(in_data.size()*sizeof(int64_t))/1'000'000;

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
    }
}
