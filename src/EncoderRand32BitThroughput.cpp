#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <chrono>
#include "parquet/schema.h"
#include "parquet/encoding.h"
#include "parquet/types.h"

#include "EncoderRoundtripTest.h"

int main(int argc, char *argv[]) {
    if(argc != 2) {
        std::cerr << "Invalid Number of Arguments! Usage: " << argv[0]
                    << " <Number of Values to write> <random delta limit>"
                    << '\n';
        return 1;
    }
    //______________Parsing_arguments___________
    //argument parsing adapted from here https://stackoverflow.com/a/2797823
    int64_t value_count; //value count
    //maximum deltas to test (fitting within different bitwidths)
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
    for(auto delta : deltas) {
        std::vector<int32_t> out_data;
        out_data.resize(value_count);

        //fill some_data with values
        std::srand(12141802); //seed with a constant value to get consistent tests
        std::vector<int32_t> in_data;
        in_data.resize(value_count);
        //initialize data with evenly spaced values
        int32_t val{0}; //value that will progressively update beetween minvalue and maxvalue
        int sign{1};
        for(auto &elem : in_data) {
            elem = val;
            //overflow would result in out-of-spec delta for this test so make sure to keep limits
            if(val >= std::numeric_limits<int32_t>::max()-delta || val <= std::numeric_limits<int32_t>::min()+delta) { 
            sign = -sign;
            }
            val+= sign*(std::rand()%(delta+1));
        }

        int64_t encMicroS;
        int64_t decMicroS;
        //get best of 100 samples
        std::tie(encMicroS, decMicroS) = encoder_roundtrip(100, in_data);
        //calculate throughput
        float dataInMb = static_cast<float>(value_count*sizeof(int32_t))/1'000'000;
        // byte / µs = byte / (s/10⁶) = byte * 10⁶ / s = MB / s
        float encMbS = static_cast<float>(value_count*sizeof(int32_t))/encMicroS;
        float decMbS = static_cast<float>(value_count*sizeof(int32_t))/decMicroS;

        encodeResult.push_back(encMbS);
        decodeResult.push_back(decMbS);

        std::cout << value_count << " values (" << dataInMb << "Mb) with a pseudorandom delta in [0;" << delta
                    << "]\nEncoding took\t" << encMicroS << "µs → ~" << encMbS << "Mb/s\n"
                    << "Decoding took\t" << decMicroS << "µs → ~" << decMbS << "Mb/s\n";
    }

    std::ofstream encodeDataFile("RandDelta_Encode_TestData.csv", std::ios::app);
    std::ofstream decodeDataFile("RandDelta_Decode_TestData.csv", std::ios::app);
    // write testrun data
    encodeDataFile << value_count;
    decodeDataFile << value_count;
    for(auto eResult : encodeResult) encodeDataFile << ", " << eResult;
    for(auto dResult : decodeResult) decodeDataFile << ", " << dResult;
    encodeDataFile << '\n';
    decodeDataFile << '\n';
    encodeDataFile.close();
    decodeDataFile.close();
}
