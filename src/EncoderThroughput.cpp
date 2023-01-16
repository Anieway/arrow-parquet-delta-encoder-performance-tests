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

        //call test function
        int64_t encMicroS;
        int64_t decMicroS;
        std::tie(encMicroS, decMicroS) = encoder_roundtrip(100, in_data);

        // byte / µs = byte / (s/10⁶) = byte * 10⁶ / s = MB / s
        float encMbS = static_cast<float>(in_data.size()*sizeof(int64_t))/encMicroS;
        float decMbS = static_cast<float>(in_data.size()*sizeof(int64_t))/decMicroS;

        encodeResult.push_back(encMbS);
        decodeResult.push_back(decMbS);

        std::cout << in_data.size() << " values (" << dataInMb << "Mb) with delta of " << delta
                    << "\nEncoding took\t" << encMicroS << "µs → ~" << encMbS << "Mb/s\n"
                    << "Decoding took\t" << decMicroS << "µs → ~" << decMbS << "Mb/s\n";
    }

    std::ofstream encodeDataFile("ConstDelta_Encode_TestData.csv", std::ios::app);
    std::ofstream decodeDataFile("ConstDelta_Decode_TestData.csv", std::ios::app);
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
