#include <iostream>
#include <fstream>
#include <queue>
#include <string>
#include <algorithm>
#include <cstdio>
#include <random>
#include <memory>
#include <set>
#include <thread>



/* function signatures */

int main(int argc, char* argv[]);
bool readInput(uint32_t&);
void write_vals(const std::unique_ptr<uint32_t[]>& values, const uint32_t, const size_t);
std::string mergeFiles(const size_t);


/* Comparison object
   describing a compare object
   for priority_queue to insert into the first place
   the minimum element
*/

struct Compare
{
    // compare 2 pairs by first element 
    bool operator() (std::pair<uint32_t, size_t>& lhs, std::pair<uint32_t, size_t>& rhs)
    {
        return lhs.first > rhs.first; // Ascending order
    }
};


/* aliases */

using ipair = std::pair<uint32_t, size_t>;
using pairvector = std::vector<ipair>;
using MinHeap = std::priority_queue< ipair, pairvector, Compare>;




/* constants */

const size_t memsize = 104857600;                        // 100 mb 104857600
const size_t chunksize = memsize / sizeof(uint32_t);   // how much int gona read? 
const std::string tmp_prefix{ "tmp_out_" };  // tmpfile prefix
const std::string merged_file{ "output" }; // output file



/// 
/// some functions outside of logic
/// 
namespace {

    //a real random generator
    void generateNumbers() {

        const uint32_t from = 1;
        const uint32_t to = 7777777;
        const uint32_t counter = 300000;

        // array for storage figures
        auto values = std::make_unique<uint32_t[]>(counter);

        std::fstream ofs("input", std::ios::binary | std::ios::out);

        try {

            for (size_t i = 0; i < counter; i++) {

                std::random_device dev;
                std::mt19937 rng(dev());
                std::uniform_int_distribution<std::mt19937::result_type> dist6(from, to); // distribution in range [1, 6]

                values[i] = dist6(rng);
            } 

            ofs.write((const char*)values.get(), sizeof(uint32_t) * counter);
            ofs.close();

            ofs.close();
        }
        catch (std::exception&)
        {
            if (ofs.is_open())
                ofs.close();

            throw std::runtime_error("runtime err: Couldn't generate random figures");
        }
    }
   
    // just read and sort input/output without memory limit
    void buffFile(const std::string&& name, std::unique_ptr<uint32_t []>& storage, uint32_t & size) {

        uint32_t tmp = 0;
        uint32_t count = 0;
        uint32_t length = 0;

        std::ifstream ifs(name, std::ios::binary | std::ios::in);

        ifs.seekg(0, std::ios::end);
        length = ifs.tellg();
        ifs.seekg(0, std::ios::beg);

        //storage
        auto inputValues = std::make_unique<uint32_t[]>(length/sizeof(uint32_t));

        ifs.read((char*)inputValues.get(), length);

        // how much bytes was read?
        uint32_t readBytes = ifs.gcount();

        //how much uints was read ? need for correct sort
        count = readBytes / sizeof(uint32_t);

        std::sort(inputValues.get(), inputValues.get()+ count);

        size = std::move(count);

        storage = std::move(inputValues);

        ifs.close();
    }

    /*just compare two files*/
    bool compareInOutFiles() {

        uint32_t size1 =0;
        uint32_t size2 = 0;

        auto in = std::make_unique<uint32_t[]>(size1);
        auto out = std::make_unique<uint32_t[]>(size2);


        std::thread th1(buffFile, "input", std::ref(in), std::ref(size1));

        std::thread th2(buffFile, "output", std::ref(out), std::ref(size2));


        th1.join();

        th2.join();
        
        return std::equal( in.get(), in.get() + size1,  out.get(), out.get() + size2);

    }
}






/// <summary>
/// basic logic's functions
/// </summary>

// write  uint array to file
void write_vals(const std::unique_ptr<uint32_t[]>& values, const uint32_t size, const size_t chunk)
{
    // tmp_out_1,  tmp_out_2 ... 
    std::string output_file = (tmp_prefix + std::to_string(chunk));

    std::fstream ofs(output_file, std::ios::binary | std::ios::out);
    
    try 
    {
        ofs.write((const char*)values.get(), size);

        ofs.close();
    }
        

    catch (std::exception&) 
    {
        if (ofs.is_open())
            ofs.close();

        throw std::runtime_error("runtime err: couldn't write " + output_file);
    }
}



/* merge all external sorted files into one
   output file (same size as original input file) */

std::string mergeFiles(const size_t chunks, const std::string& merge_file)
{

    std::ofstream ofs(merge_file.c_str(), std::ios::binary | std::ios::out);

    MinHeap  minHeap;

    try {

        // unique array of ifstreams 
        auto ifs_tempfiles = std::make_unique<std::ifstream[]>(chunks);


        for (size_t i = 1; i <= chunks; i++)
        {
            // generate a unique name for temp file (temp_out_1 , temp_out_2 ..) 
            std::string sorted_file = (tmp_prefix + std::to_string(i));

            // open an input file stream object for each name
            ifs_tempfiles[i - 1].open(sorted_file.c_str(), std::ios::binary | std::ios::in); // bind to tmp_out_{i}

            // get val from temp file
            if (ifs_tempfiles[i - 1].is_open())
            {
                uint32_t topValue = 0;
                uint32_t count = 0;

                ifs_tempfiles[i - 1].read((char*)&topValue, sizeof(uint32_t));

                uint32_t readBytes = ifs_tempfiles[i - 1].gcount();

                count += readBytes / sizeof(uint32_t);

                if (count) {
                    
                    ipair top(topValue, (i - 1)); // 2nd value is tempfile number   

                    minHeap.push(top);   //  minHeap autosorts
                }
            }
        }


        while (minHeap.size() > 0)
        {
            uint32_t next_val = 0;
            uint32_t count = 0;

            ipair min_pair = minHeap.top(); // get min
            
            minHeap.pop();      // delete and when go to write the next number

            ofs.write((const char*)&min_pair.first, sizeof(uint32_t));

            // in the current file we have to find the next number
            ifs_tempfiles[min_pair.second].read((char*)&next_val, sizeof(uint32_t));

            //read next
            uint32_t readBytes = ifs_tempfiles[min_pair.second].gcount();

            count = readBytes / sizeof(uint32_t);

            if (count) //is successful read? have to see "1"
            {
                ipair np(next_val, min_pair.second);

                minHeap.push(np);
            }

        }
        

        // close and delete open files
        for (size_t i = 1; i <= chunks; i++)
        {
            ifs_tempfiles[i - 1].close();

            std::string remFile = tmp_prefix + std::to_string(i);

            std::remove(remFile.c_str());
        }

        ofs.close();

    }
    catch (std::exception&)
    {
        if (ofs.is_open())
            ofs.close();

        throw std::runtime_error("runtime err: sm error in the merge logic");
    }

    return merged_file;  // string
}

bool readInput(uint32_t& chunk) {

    bool readSuccessed = false;

    uint32_t val = 0;      // int  for reading
    uint32_t count = 0;    // count reads

    std::ifstream ifs("input", std::ios::binary | std::ios::in);

    try {
        if (ifs.fail())
        {
            throw std::runtime_error("error opening input\n");
        }

        std::cout << "internal buffer is " << memsize << " bytes" << "\n";


        ifs.seekg(0, std::ios::beg);


        while (!ifs.eof()) {

            auto inputValues = std::make_unique<uint32_t[]>(chunksize);

            ifs.read((char*)inputValues.get(), memsize);

            uint32_t readBytes = ifs.gcount();

            count = readBytes / sizeof(uint32_t);

            if (count)    readSuccessed = true;

            std::sort(inputValues.get(), inputValues.get() + count);

            // go to write part of input to tmp file
            write_vals(inputValues, readBytes, chunk); // output vals to

            if (ifs.eof()) break;  //otherwise chunk will be increased
            chunk++;
            count = 0;
        }
    }
    catch (std::runtime_error&) {
        throw;
    }
    catch (std::exception&) {
        throw;
    }

    ifs.close();

    return readSuccessed;

}

int main(int argc, char* argv[])
{
    try {

        static uint32_t chunk = 1;    // counter (which chunk)

        // first of all let's generate random figures
        //generateNumbers();
        

        // open input file for reading
        if ( readInput(chunk))
            std::cout << "Sorted output is in file: " << mergeFiles(chunk, merged_file) << "\n";        // go to merge files
        else
            std::cout << "no data found\n";

        // compare input output  without memory limit
        //std::cout<< "equals of input and output == " <<compareInOutFiles() << std::endl;


    }
    catch (std::runtime_error& re)
    {
        std::cout << re.what();
        return EXIT_FAILURE;
    }
    catch (std::exception& ex) 
    {
        std::cout << ex.what();
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
