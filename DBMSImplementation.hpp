#include <array>
#include <cstdlib>
#include <tuple>
#include <variant>
#include <vector>

using AttributeValue = std::variant<long, double, char const*>;
using Tuple = std::vector<AttributeValue>;
using Relation = std::vector<Tuple>;
using Result = std::vector<std::vector<std::vector<int> > >;
std::vector<int> temp_comparison_relation;
Relation temp_comparison_relation_doubles;

/**
 * 0 islong, 1 is double, 2 is a c-string
 */
inline size_t getAttributeValueType(AttributeValue const& value) { return value.index(); }
inline long getLongValue(AttributeValue const& value) { return std::get<long>(value); }
inline double getdoubleValue(AttributeValue const& value) { return std::get<double>(value); }
inline char const* getStringValue(AttributeValue const& value) {
  return std::get<char const*>(value);
}
inline size_t getNumberOfValuesInTuple(Tuple const& t) { return t.size(); }
inline size_t getNumberOfTuplesInRelation(Relation const& t) { return t.size(); }

class DBMSImplementation {
  Relation large1;
  Relation large2;
  Relation small;
  std::vector<Relation> relations;
  //Relation specific vectors where indexes for each data will be stored in a separate vector, one for each data type
  std::vector<std::vector<int>> index_large1, index_large2, index_small;
  //Vector containing the transformed values of the first column for each relation
  std::vector<int> large1_values_col0, large2_values_col0, small_values_col0;



public:

  int hash_string (std::string word){
        // function to convert a string to an int
        int sum = 0;
        for (int k = 0; k < word.length(); k++)
                sum = sum + int(word[k]);
        return  sum;
    }

  void loadData(Relation const* large1,
                Relation const* large2, // NOLINT(bugprone-easily-swappable-parameters)
                Relation const* small)  // NOLINT(bugprone-easily-swappable-parameters)
  {
    this->large1 = *large1;
    this->large2 = *large2;
    this->small = *small;


    relations = {this->large1, this->large2, this->small}; //storing all relations in a vector
    
    
    std::vector<int> v; //initializing the index vectors by appending 3 vectors to them
     for (int k = 0; k<3; k++){
        index_large1.push_back(v);
        index_large2.push_back(v);
        index_small.push_back(v);
    }
    for (int k = 0; k < 3; k++) { // iteration over all tables

      std::vector<int> index_long, index_double, index_string; // temporary vectors in which indexes are stored
      std::vector<int> temp_vals; // temporary vector for the values of the first column

      int size_array = relations[k].size();

      for (int i = 0; i < size_array; i++){ // iterate over all the tuples in a relation
          int attributeValueType = getAttributeValueType(relations[k][i][0]); // attribute type of first column

          if(attributeValueType == 0) { // case when attribute is long
                // check for which table the append has to be done
                if(k == 0){index_large1[0].push_back(i);
                           large1_values_col0.push_back(getLongValue(relations[k][i][0]));
                          }
                else if(k==1){index_large2[0].push_back(i);
                              large2_values_col0.push_back(getLongValue(relations[k][i][0]));
                             }
                else {index_small[0].push_back(i);
                      small_values_col0.push_back(getLongValue(relations[k][i][0]));
                     }
          }
          else if(attributeValueType == 1) { // case when attribute is double
                double temp_val = getdoubleValue(relations[k][i][0]);
                if (temp_val > 0) {//convert double to long
                    int temp_val = int(temp_val+0.5);
                }
                else {
                    int temp_val = int(temp_val-0.5);
                }
                // check for which table the append has to be done
                if(k == 0){index_large1[1].push_back(i);
                           large1_values_col0.push_back(temp_val);
                          }
                else if(k==1){index_large2[1].push_back(i);
                             large2_values_col0.push_back(temp_val); 
                             }
                else {index_small[1].push_back(i);
                      small_values_col0.push_back(temp_val); 
                     }
                
                
          }
          else if(attributeValueType == 2) { // case when attribute is string
                //check if value is nullptr
                if (std::get<char const*>(relations[k][i][0]) != nullptr){
                // check for which table the append has to be done
                if(k == 0){index_large1[2].push_back(i); 
                           // append hashed string
                           large1_values_col0.push_back(int(hash_string(getStringValue(relations[k][i][0])))); 
                          }
                else if(k==1){index_large2[2].push_back(i);
                              large2_values_col0.push_back(int(hash_string(getStringValue(relations[k][i][0]))));
                             }
                else {index_small[2].push_back(i);
                      small_values_col0.push_back(int(hash_string(getStringValue(relations[k][i][0]))));
                     }
                }
                else {
                    if(k == 0){ // append 0 to the vector of values if the attribute is nullptr
                           large1_values_col0.push_back(0);
                          }
                    else if(k==1){
                              large2_values_col0.push_back(0);
                             }
                    else {
                          small_values_col0.push_back(0);
                         }
                }
          }
    }
    }

  }


    static int compare_longs (const void * a, const void * b){
        //comparator function for qsort
        int aa = *((int *)a), bb = *((int *)b);
        long val1 = temp_comparison_relation[aa];
        long val2 = temp_comparison_relation[bb];
        return (val1 - val2);
    }

    static int compare_doubles (const void * a, const void * b){
        // comparator function for doubles
        // needed as there can be values such as 9.1, 9.3 so can't use the transformed version
        int aa = *((int *)a), bb = *((int *)b);
        double val1 = getdoubleValue(temp_comparison_relation_doubles[aa][0]);
        double val2 = getdoubleValue(temp_comparison_relation_doubles[bb][0]);
        return ( val1 - val2 );
    }


    std::vector<std::vector<int> > sort_relation_indices(std::vector<std::vector<int> > index_tables, std::vector<int> table_values, Relation relation){
        //sorting all the index tables for a given relation
        temp_comparison_relation = table_values;
        temp_comparison_relation_doubles = relation;
        for (int k=0; k<3; k++){ // iterate over the indexes for each type of attribute
            // omit the index list for nulls as it will not be used
            int size_of_index_table = index_tables[k].size();
            if (k == 1){ // if the value is a double compare the actual value, not the approximation
                qsort(&index_tables[k][0], size_of_index_table, sizeof(int), compare_doubles); // using qsort as the sorting algorithm of choice
            }
            else {

            qsort(&index_tables[k][0], size_of_index_table, sizeof(int), compare_longs); // using qsort as the sorting algorithm of choice
        }
        }
        return index_tables;
    }


  std::vector<std::vector<int> > sort_merge_join(std::vector<std::vector<int> > index_tables_left, Result index_tables_right, std::vector<int> table_values_left, std::vector<int> table_values_right){
    // the function that realizes the sort merge between the mid-view obtained by joining large2 and small and the large 1 relation
    // inputs:
    // - index_tables_left - list of indices per data type for the table on the left, large 1
    // - index_tables_right - list of indices per data type for the table on the right, midview - for each join there are kept 2 indices, one for the small table and one for the large 2 table
    // - table_values_left - vector containing the transformed values for column 0 for the large 1 table
    // - table_values_right- vector containing the transformed values for column 0 for the small table, using this as the values will be indentical to the ones of large 2

    std::vector<std::vector<int> >  join_result; // vector containing the result of the join, each tuple contains the indices from all 3 tables, but does not take in account data type anymore

    for (int k=0; k<3; k++){ // iterate over the list of indices for each data type

        int size_of_index_table_right = index_tables_right[k].size();
        int size_of_index_table_left = index_tables_left[k].size();

        int leftI = 0;
        int rightI = 0;
        //Classic sort merge join as implemented in the slides
        while (leftI < size_of_index_table_left && rightI < size_of_index_table_right) {

            int leftIndex = index_tables_left[k][ leftI];
            int rightIndex = index_tables_right[k][ rightI][0]; // needed the extra index due to each input containing 2 indices

            if(table_values_left[leftIndex] < table_values_right[rightIndex]){ // comparing values from relations
                leftI++;
            }
            else if(table_values_right[rightIndex] < table_values_left[leftIndex]) {
                rightI++;
            }
            else {
                join_result.push_back({leftIndex, index_tables_right[k][rightI][0], index_tables_right[k][rightI][1]});
                rightI++;
                //duplicate handling
                if (rightI < size_of_index_table_right){ // check if the right index is not pointing to the last value in the vector

                rightIndex = index_tables_right[k][rightI][0]; // update the index for the values tables with the new incremented rightI index

                while(table_values_left[leftIndex] == table_values_right[rightIndex] && rightI < size_of_index_table_right){ //check that the attribute values still match and that the right index is not pointing to the last value in the vector
                join_result.push_back({leftIndex, index_tables_right[k][rightI][0], index_tables_right[k][rightI][1]}); // pushing the tuple corresponding to the duplicate to the intermediary table

                rightIndex = index_tables_right[k][rightI][0]; // update the right index for the values table

                rightI++; // increment the right index for the index table

                }
                }
                leftI++; // increment the left index for the index table
                }
        }
    }
    return join_result;
  }


  void hash_join(std::vector<std::vector<int> > index_tables_small, std::vector<std::vector<int> > index_tables_large, std::vector<int> table_values_small, std::vector<int> table_values_large, Result &hash_join_result){
    // Performs hash join between the small and the large 2 table
    // Inputs: 
    // - index_tables_small - list of indices per data type for the table on the left, small 
    // - index_tables_right - list of indices per data type for the table on the right, large 2
    // - table_values_left - vector containing the transformed values for column 0 for the small table
    // - table_values_right- vector containing the transformed values for column 0 for the large 2 table
    // - hash_join_result - vector which will contain the result of the join
    
    // The implementation follows closely the one from the slides
    for (int k=0; k<3; k++){
        
        int buildSideSize = index_tables_small[k].size();
        
        std::vector<int> hashTable(buildSideSize); // initialize the hashTable as a vector with predefined size
        
        int probeSideSize = index_tables_large[k].size();
        
        if (buildSideSize!=0 && probeSideSize!=0){ //check that none of the list of indexes for a data type is empty
            
        //fill hashTable with empty characters
        for (int i = 0; i < buildSideSize; i++){ 
            hashTable[i] = ' ';
        }
        
        // build hashTable
        for(int i = 0; i < buildSideSize; i++) { 
            
            int buildIndex = index_tables_small[k][i];
            int buildInput = table_values_small[buildIndex];
            //used hash function is the module computed with the buildSideSize
            int hashValue = int(buildInput % buildSideSize);  
            
            while(hashTable[hashValue] != ' '){ // iterate over the HashTable until an empty slot is found
                int hashTemp = hashValue + 1;
                hashValue = int(hashTemp % buildSideSize); 
            }
            // fill hashTable with values 
            hashTable[hashValue] = buildIndex;
        }
        
        // Linear probing 
        for(int i = 0; i < probeSideSize; i++) { // iterate over all the values in the probe side
            
            int probeIndex = index_tables_large[k][i];
            int probeInput = table_values_large[probeIndex];
            int hashValue = int(probeInput % buildSideSize); // apply hashing function
            
            int t = 0; // variable used to check if we have already iterated over the whole hashTable
            while(table_values_small[hashTable[hashValue]] != probeInput && t<buildSideSize) { //search for a match between the values in the small table and the large 1 table for the given hash value
            // if not found, do linear probing 
                int hashTemp = hashValue + 1;
                hashValue = int(hashTemp % buildSideSize); 
                t++;
            }
            
            if(table_values_small[hashTable[hashValue]] == probeInput) {
                // if match found, append to the list of temporary indexes
                hash_join_result[k].push_back({hashTable[hashValue], probeIndex}); 
                
                //duplicate handling
                int nextHashValTemp = hashValue+1; // define temporary value and increment the hash value
                int hashValueTemp = int(nextHashValTemp % buildSideSize); // apply hashing function
                
                while(table_values_small[hashTable[hashValueTemp]] == probeInput && hash_join_result[k].size() < buildSideSize){ // check if the incremented value on the build side still matches the value in the probe side
                // since only the build side contains duplicates we only check for the next values in the build side
                    
                    hash_join_result[k].push_back({hashTable[hashValueTemp], probeIndex}); // append duplicate value
                    
                    // repeat the increment operation to check if the next value is also duplicate
                    nextHashValTemp = hashValueTemp + 1; 
                    hashValueTemp = int(nextHashValTemp % buildSideSize);
                
                }
            }
        }
        }
        else { // if build or probe sides are empty then just continue as the result vector is already empty for the index
            continue;
        }
     }
  }



  long runQuery(long threshold = 9L) {
    auto sum = 0L;
    
    Result hash_join_result;
    std::vector<std::vector<int> > vbig;
    for (int k = 0; k<3; k++){
        hash_join_result.push_back(vbig);
    }
    // sort the indexes per data type for each relation
    // did not manage to do it inplace with qsort
    std::vector<std::vector<int>> index_large1_sorted = sort_relation_indices(index_large1, large1_values_col0, large1);
    std::vector<std::vector<int>> index_large2_sorted = sort_relation_indices(index_large2, large2_values_col0, large2);
    std::vector<std::vector<int>> index_small_sorted = sort_relation_indices(index_small, small_values_col0, small);

    //perform the hash join between the small and the large 2 relations
    hash_join(index_small_sorted, index_large2_sorted, small_values_col0, large2_values_col0, hash_join_result);
    //perform the sort merge join between the large 1 and the midview relations
    std::vector<std::vector<int> > final_join_result = sort_merge_join(index_large1_sorted, hash_join_result, large1_values_col0, small_values_col0);


    int size_res = final_join_result.size();

    for (int k = 0; k < size_res; k++){ // iterate over all the values in the result vector
                //collect all the values for the first column for all the tables
                long val_col1_large1 = getLongValue(large1[final_join_result[k][0]][1]);
                long val_col1_small = getLongValue(small[final_join_result[k][1]][1]);
                long val_col1_large2 = getLongValue(large2[final_join_result[k][2]][1]);
                if (val_col1_large1+val_col1_small+val_col1_large2 > threshold){ //apply threshild condition
                   //collect all the values for the second column for all the tables
                   long val_col2_large1 = getLongValue(large1[final_join_result[k][0]][2]);
                   long val_col2_small = getLongValue(small[final_join_result[k][1]][2]);
                   long val_col2_large2 = getLongValue(large2[final_join_result[k][2]][2]);
                   sum += val_col2_large1*val_col2_small*val_col2_large2; // sum the products of the values in the second table
                 }

    }
    return sum;

  }
};
