#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstring>
#include <vector>
#include <array>
#include <chrono>

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
    std::vector<int64_t> timingResult;
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

        std::vector<int64_t> out_data;
        out_data.resize(value_count);

        //calculate throughput
        float dataInMb = static_cast<float>(in_data.size()*sizeof(int64_t))/1'000'000;

        //call test function
        int64_t timeMicroS;

        //TODO time memcopy
        const auto start = std::chrono::steady_clock::now();
        std::memcpy(out_data.data(), in_data.data(), value_count*sizeof(int64_t));
        const auto end = std::chrono::steady_clock::now();

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
            timeMicroS =
            std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();

            // byte / µs = byte / (s/10⁶) = byte * 10⁶ / s = MB / s
            float MbS = static_cast<float>(in_data.size()*sizeof(int64_t))/timeMicroS;
            timingResult.push_back(MbS);

            std::cout << in_data.size() << " values (" << dataInMb << "Mb) with delta of " << delta
                        << "\nCopy took\t" << timeMicroS << "µs → ~" << MbS << "Mb/s\n";
        }
        else {
            std::cerr << "Validation complete. Unsuccessful:\n"
                        << error_count << " missmatched values!\n";
        }
    }

    std::ofstream testDataFile("Memcopy_TestData.csv", std::ios::app);
    // write testrun data
    testDataFile << value_count;
    for(auto eResult : timingResult) testDataFile << ", " << eResult;
    testDataFile << '\n';
    testDataFile.close();
}
