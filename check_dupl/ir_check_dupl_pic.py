Mã này thực hiện quá trình gom nhóm các giá trị trong một cột của DataFrame dựa trên độ tương đồng giữa chúng và tạo một cột mới để lưu trữ key của nhóm tương ứng. Dưới đây là giải thích từng bước:

1. `groups = {}`: Tạo một từ điển rỗng để lưu trữ các nhóm. Trong từ điển này, key là tên của nhóm và giá trị là danh sách các index của các dòng thuộc nhóm đó.

2. `key_column_name = f"{column_name}_GroupKey"`: Tạo tên cho cột mới để lưu trữ key của nhóm. Cột này sẽ được thêm vào DataFrame và có tên được xây dựng bằng cách thêm "_GroupKey" vào tên của cột gốc.

3. `df[key_column_name] = ""`: Thêm cột mới vào DataFrame và gán giá trị mặc định là chuỗi rỗng cho tất cả các dòng. Cột này sẽ được sử dụng để lưu trữ key của nhóm.

4. `for index, row in df.iterrows():`: Bắt đầu vòng lặp qua từng dòng trong DataFrame.

5. `added_to_group = False`: Biến cờ để theo dõi xem dòng hiện tại đã được thêm vào một nhóm nào đó chưa.

6. `for key, group in groups.items():`: Lặp qua từng nhóm trong từ điển `groups`.

7. `for group_index in group:`: Lặp qua từng index của dòng trong nhóm.

8. `similarity = fuzz.ratio(row[column_name], df.at[group_index, column_name])`: Tính độ tương đồng giữa giá trị của dòng hiện tại và giá trị của dòng trong nhóm sử dụng hàm `fuzz.ratio` từ thư viện fuzzywuzzy.

   - Nếu độ tương đồng lớn hơn ngưỡng (50% trong trường hợp này), có thể coi hai giá trị là giống nhau và dòng hiện tại sẽ được thêm vào nhóm.

9. Nếu dòng hiện tại chưa được thêm vào bất kỳ nhóm nào (`added_to_group` là False), tạo một nhóm mới cho dòng hiện tại:

   - `key = f"Group_{len(groups) + 1}"`: Tạo tên mới cho nhóm dựa trên số lượng nhóm hiện tại.

   - `df.at[index, key_column_name] = key`: Gán key của nhóm cho cột mới của dòng hiện tại.

   - `groups[key] = [index]`: Thêm dòng hiện tại vào danh sách index của nhóm mới trong từ điển.

Cuối cùng, sau khi vòng lặp kết thúc, DataFrame sẽ được cập nhật với cột mới (`key_column_name`) chứa key của nhóm tương ứng cho mỗi dòng. Các dòng có giá trị tương đồng lớn hơn 50% sẽ thuộc cùng một nhóm.




import pandas as pd
from fuzzywuzzy import fuzz

def group_similar_values(df, column_name, threshold=50):
    groups = {}
    key_column_name = f"{column_name}_GroupKey"

    # Tạo một cột key để lưu trữ nhóm
    df[key_column_name] = ""

    for index, row in df.iterrows():
        added_to_group = False

        for key, group in groups.items():
            for group_index in group:
                similarity = fuzz.ratio(row[column_name], df.at[group_index, column_name])
                if similarity > threshold:
                    df.at[index, key_column_name] = key
                    groups[key].append(index)
                    added_to_group = True
                    break

        if not added_to_group:
            key = f"Group_{len(groups) + 1}"
            df.at[index, key_column_name] = key
            groups[key] = [index]

    return df

# Tạo DataFrame mẫu
data = {
    'Column1': ['apple', 'aple', 'banana', 'orange', 'grape', 'kiwi', 'pear'],
    'Column2': ['red', 'green', 'yellow', 'orange', 'purple', 'brown', 'green']
}

df = pd.DataFrame(data)

# Áp dụng hàm group_similar_values cho cột 'Column1'
df_result = group_similar_values(df, column_name='Column1', threshold=50)

# Hiển thị DataFrame kết quả
print(df_result)





import pandas as pd
from fuzzywuzzy import fuzz

def group_similar_values_within_large_groups(df, large_group_column, column_name, threshold=50):
    small_groups = {}

    for large_group, group_df in df.groupby(large_group_column):
        # Lặp qua từng nhóm lớn
        large_group_key = f"_SmallGroupKey"
        group_df[large_group_key] = ""

        for index, row in group_df.iterrows():
            added_to_small_group = False

            for key, small_group in small_groups.items():
                for small_group_index in small_group:
                    similarity = fuzz.ratio(row[column_name], df.at[small_group_index, column_name])
                    if similarity > threshold:
                        df.at[index, large_group_key] = key
                        small_groups[key].append(index)
                        added_to_small_group = True
                        break

            if not added_to_small_group:
                key = f"SmallGroup_{len(small_groups) + 1}"
                df.at[index, large_group_key] = key
                small_groups[key] = [index]

    return df

# Tạo DataFrame mẫu
data = {
    'LargeGroup': ['A', 'A', 'B', 'B', 'C', 'C'],
    'ValueColumn': ['apple', 'aple', 'banana', 'orange', 'grape', 'kiwi']
}

df = pd.DataFrame(data)

# Áp dụng hàm group_similar_values_within_large_groups
df_result = group_similar_values_within_large_groups(df, large_group_column='LargeGroup', column_name='ValueColumn', threshold=50)

# Hiển thị DataFrame kết quả
print(df_result)




Để thực hiện so sánh giữa các dòng trong một cột khác theo từng nhóm lớn (group) của một cột key đã cho trong DataFrame và tạo thêm một cột key để phân biệt các nhóm nhỏ hơn nếu độ tương đồng lớn hơn 50%, bạn có thể sử dụng thư viện pandas và fuzzywuzzy. Dưới đây là một ví dụ:

Trước khi sử dụng mã dưới đây, bạn cần cài đặt thư viện pandas và fuzzywuzzy bằng cách chạy lệnh sau:

```bash
pip install pandas fuzzywuzzy
```

Sau đó, bạn có thể sử dụng mã sau:

```python
import pandas as pd
from fuzzywuzzy import fuzz

def group_similar_values_within_large_groups(df, large_group_column, column_name, threshold=50):
    small_groups = {}

    for large_group, group_df in df.groupby(large_group_column):
        # Lặp qua từng nhóm lớn
        large_group_key = f"{large_group}_SmallGroupKey"
        group_df[large_group_key] = ""

        for index, row in group_df.iterrows():
            added_to_small_group = False

            for key, small_group in small_groups.items():
                for small_group_index in small_group:
                    similarity = fuzz.ratio(row[column_name], df.at[small_group_index, column_name])
                    if similarity > threshold:
                        df.at[index, large_group_key] = key
                        small_groups[key].append(index)
                        added_to_small_group = True
                        break

            if not added_to_small_group:
                key = f"SmallGroup_{len(small_groups) + 1}"
                df.at[index, large_group_key] = key
                small_groups[key] = [index]

    return df

# Tạo DataFrame mẫu
data = {
    'LargeGroup': ['A', 'A', 'B', 'B', 'C', 'C'],
    'ValueColumn': ['apple', 'aple', 'banana', 'orange', 'grape', 'kiwi']
}

df = pd.DataFrame(data)

# Áp dụng hàm group_similar_values_within_large_groups
df_result = group_similar_values_within_large_groups(df, large_group_column='LargeGroup', column_name='ValueColumn', threshold=50)

# Hiển thị DataFrame kết quả
print(df_result)
```

Trong ví dụ này:

- Hàm `group_similar_values_within_large_groups` nhận DataFrame, tên của cột key của nhóm lớn (`large_group_column`), tên của cột giá trị cần so sánh (`column_name`), và ngưỡng độ tương đồng.

- Đối với mỗi nhóm lớn, hàm tạo một cột mới để lưu trữ key của nhóm nhỏ trong DataFrame.

- Sau đó, nó lặp qua từng dòng trong nhóm lớn và so sánh giá trị của cột giá trị với các dòng trong nhóm lớn.

- Nếu độ tương đồng lớn hơn ngưỡng, dòng hiện tại được thêm vào nhóm nhỏ và key của nhóm nhỏ được gán cho cột mới.

- Nếu dòng chưa được thêm vào bất kỳ nhóm nhỏ nào, tạo một nhóm nhỏ mới cho dòng đó.

Cuối cùng, DataFrame sẽ được cập nhật với cột mới chứa key của nhóm nhỏ tương ứng cho mỗi dòng trong nhóm lớn. Các dòng có giá trị tương đồng lớn hơn 50% sẽ thuộc cùng một nhóm nhỏ.