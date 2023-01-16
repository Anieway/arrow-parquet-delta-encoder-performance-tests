#include <iostream>
#include <fstream>
#include <stdexcept>
#include <vector>
#include "arrow/io/file.h"
#include "parquet/api/reader.h"

#include "EncoderRoundtripTest.h"

int main(int argc, char *argv[]) {
    if(argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <path/to/data.parquet>\n";
        return EXIT_FAILURE;
    }
    //_______________Reading_values_____________
    constexpr size_t readBatchSize{100}; //number of values read at a time
    // WARNING: Reading one value at a time is bugged and gives only 0!!!
    int errcount = 0;
    int readTotal = 0;
    std::vector<int64_t> data;
    data.resize(readBatchSize);
    //adapted from apache/arrow/cpp/examples/parquet/low_level_api/reader_writer.cc
    try{
        // Create a ParquetReader instance
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
                parquet::ParquetFileReader::OpenFile(argv[1], false);

        // Get the File MetaData
        std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();

        // Get the number of RowGroups
        int num_row_groups = file_metadata->num_row_groups();
        // Get the number of Columns
        int num_columns = file_metadata->num_columns();
        
        std::cout << "Data has " << num_row_groups << "row-groups and "
                    << num_columns << " columns.\n";

        // Iterate over all the RowGroups in the file
        for (int r = 0; r < num_row_groups; ++r) {
            // Get the RowGroup Reader
            std::shared_ptr<parquet::RowGroupReader> row_group_reader =
                parquet_reader->RowGroup(r);

            int64_t values_read = 0;
            int64_t rows_read = 0;
            int16_t definition_level;
            int16_t repetition_level;
            std::shared_ptr<parquet::ColumnReader> column_reader;

            // Get the Column Reader for the Int64 column
            column_reader = row_group_reader->Column(0);
            parquet::Int64Reader* int64_reader =
                static_cast<parquet::Int64Reader*>(column_reader.get());
            while (int64_reader->HasNext()) {
                std::array<int64_t, readBatchSize> values;
                // The number of rows read is returned.
                // values_read contains the number of non-null rows
                rows_read = int64_reader->ReadBatch(readBatchSize, &definition_level, &repetition_level,
                                                    values.data(), &values_read);
                // add read values to data
                for(auto val : values) {
                    data.push_back(val);
                }
                readTotal+=values_read;
            }
        }

    } catch (const std::exception& e) {
        std::cerr << "Parquet read error: " << e.what() << std::endl;
        return -1;
    }
    //check data volume
    if(readTotal != data.size()) {
        std::cerr << "Data volume invariant violated!\n";
        return 1;
    }
    //data recieved
    //calculate throughput
    float dataInMb = static_cast<float>(data.size()*sizeof(int64_t))/1'000'000;

    //call test function
    int64_t encMicroS;
    int64_t decMicroS;
    std::tie(encMicroS, decMicroS) = encoder_roundtrip(100, data);

    // byte / µs = byte / (s/10⁶) = byte * 10⁶ / s = MB / s
    float encMbS = static_cast<float>(data.size()*sizeof(int64_t))/encMicroS;
    float decMbS = static_cast<float>(data.size()*sizeof(int64_t))/decMicroS;

    std::cout << data.size() << " values (" << dataInMb << "Mb) from file " << argv[1]
                << "\nEncoding took\t" << encMicroS << "µs → ~" << encMbS << "Mb/s\n"
                << "Decoding took\t" << decMicroS << "µs → ~" << decMbS << "Mb/s\n";
    //append testdata to file for this test
    std::ofstream encodeDataFile("File_Encode_TestData.csv", std::ios::app);
    std::ofstream decodeDataFile("File_Decode_TestData.csv", std::ios::app);
    encodeDataFile << data.size() << ", " << encMbS << '\n';
    decodeDataFile << data.size() << ", " << decMbS << '\n';
    encodeDataFile.close();
    decodeDataFile.close();
}
