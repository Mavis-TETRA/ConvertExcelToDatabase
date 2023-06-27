import pandas as pd
import numpy as np
import pyodbc as po
import streamlit as st
import configparser
import logging
import datetime
import threading


def DataAlreadyExists(Data_already_exists, df, COUNT_ROW_DATA,columns):
    Data_already_exists['ID'].append(str(df.loc[COUNT_ROW_DATA, [columns[0]]].iloc[0]))
    Data_already_exists['First_Name'].append(str(df.loc[COUNT_ROW_DATA, [columns[1]]].iloc[0]))
    Data_already_exists['Last_Name'].append(str(df.loc[COUNT_ROW_DATA, [columns[2]]].iloc[0]))
    Data_already_exists['Gender'].append(str(df.loc[COUNT_ROW_DATA, [columns[3]]].iloc[0]))
    Data_already_exists['Date_B'].append(str(df.loc[COUNT_ROW_DATA, [columns[4]]].iloc[0]))
    Data_already_exists['Month_B'].append(str(df.loc[COUNT_ROW_DATA, [columns[5]]].iloc[0]))
    Data_already_exists['Year_B'].append(str(df.loc[COUNT_ROW_DATA, [columns[6]]].iloc[0]))
    Data_already_exists['Birthplace'].append(str(df.loc[COUNT_ROW_DATA, [columns[7]]].iloc[0]))
    Data_already_exists['Nationality'].append(str(df.loc[COUNT_ROW_DATA, [columns[8]]].iloc[0]))
    Data_already_exists['Citizen_Id'].append(str(df.loc[COUNT_ROW_DATA, [columns[9]]].iloc[0]))
    Data_already_exists['Province_Id'].append(str(df.loc[COUNT_ROW_DATA, [columns[10]]].iloc[0]))
    Data_already_exists['School_Id'].append(str(df.loc[COUNT_ROW_DATA, [columns[11]]].iloc[0]))
    Data_already_exists['Class_Name'].append(str(df.loc[COUNT_ROW_DATA, [columns[12]]].iloc[0]))
    Data_already_exists['Phone'].append(str(df.loc[COUNT_ROW_DATA, [columns[13]]].iloc[0]))
    Data_already_exists['Email'].append(str(df.loc[COUNT_ROW_DATA, [columns[14]]].iloc[0]))
    Data_already_exists['Address'].append(str(df.loc[COUNT_ROW_DATA, [columns[15]]].iloc[0]))
    Data_already_exists['Ad_Exam_Results'].append(str(df.loc[COUNT_ROW_DATA, [columns[16]]].iloc[0]))
    Data_already_exists['HS_Edu'].append(str(df.loc[COUNT_ROW_DATA, [columns[17]]].iloc[0]))
    Data_already_exists['Continuing_Edu'].append(str(df.loc[COUNT_ROW_DATA, [columns[18]]].iloc[0]))


def Datamanagerment(config, CURSOR,pd,table_name,search_query) :
    Data = {'ID_Serial': [],'ID': [],'First_Name': [],'Last_Name': [],'Gender': [],'Date_B': [],'Month_B': [],'Year_B': [],'Birthplace': [],'Nationality': [],'Citizen_Id': [],'Province_Id': [],'School_Id': [],'Class_Name': [],'Phone': [],'Email': [],'Address': [],'Ad_Exam_Results': [],'HS_Edu': [],'Continuing_Edu': [],'Status': [],'CreateAt': [],'UpdateAt': []
    }

    
    query = f"SELECT * FROM {table_name}"
    CURSOR.execute(query)
    results = CURSOR.fetchall()
    # CURSOR.close()
    # CONN.close()
    for item in results:
        Data['ID_Serial'].append(str(item[0]))
        Data['ID'].append(str(item[1]))
        Data['First_Name'].append(str(item[2]))
        Data['Last_Name'].append(str(item[3]))
        Data['Gender'].append(str(item[4]))
        Data['Date_B'].append(str(item[5]))
        Data['Month_B'].append(str(item[6]))
        Data['Year_B'].append(str(item[7]))
        Data['Birthplace'].append(str(item[8]))
        Data['Nationality'].append(str(item[9]))
        Data['Citizen_Id'].append(str(item[10]))
        Data['Province_Id'].append(str(item[11]))
        Data['School_Id'].append(str(item[12]))
        Data['Class_Name'].append(str(item[13]))
        Data['Phone'].append(str(item[14]))
        Data['Email'].append(str(item[15]))
        Data['Address'].append(str(item[16]))
        Data['Ad_Exam_Results'].append(str(item[17]))
        Data['HS_Edu'].append(str(item[18]))
        Data['Continuing_Edu'].append(str(item[19]))
        Data['Status'].append(str(item[20]))
        Data['CreateAt'].append(str(item[21]))
        Data['UpdateAt'].append(str(item[22]))
        

    dtf = pd.DataFrame(Data)
    
    return dtf


    
# thread_zalo = threading.Thread(target=consume_messages_zalo)
# thread_zalo.start()
    
    
def ChangeAndDelete(st,CURSOR,search_query,config,pd,CONN) :
    table_name = config.get('table', 'table_name')
    
    dtf = Datamanagerment(config, CURSOR,pd,table_name,search_query)
    if search_query != '':

        filtered_df = dtf[dtf['Citizen_Id'].str.contains(search_query, case=False)]
        if st.button("Reload"):
            st.dataframe(filtered_df)
        else:
            st.dataframe(filtered_df)
    else: 
        if st.button("Reload"):
            st.dataframe(dtf)
        else:
            st.dataframe(dtf)
    st.title("Chỉnh Sửa Dữ Liệu")
    # row_index = st.number_input("Chỉnh sửa dữ liệu dựa vào Citizen_Id : ", value=0)
    changeData = st.text_input("Chỉnh sửa dữ liệu dựa vào Citizen_Id : ", "", help="Nhập thông tin vào ô trống.")
    filtered_change = dtf[dtf['Citizen_Id'].str.contains(changeData, case=False)]

    if filtered_change.empty:
        st.write("Không tìm thấy sinh viên.")
    elif changeData == '':
        st.write("")
    else:
        for index, row in filtered_change.iterrows():
            id = st.text_input("ID", f"{row['ID']}")
            first_name = st.text_input("First_Name", f"{row['First_Name']}")
            last_name = st.text_input("Last_Name", f"{row['Last_Name']}")
            gender = st.text_input("Gender", f"{row['Gender']}")
            date_b = st.text_input("Date_B", f"{row['Date_B']}")
            month_b = st.text_input("Month_B", f"{row['Month_B']}")
            year_b = st.text_input("Year_B", f"{row['Year_B']}")
            birthplace = st.text_input("Birthplace", f"{row['Birthplace']}")
            nationality = st.text_input("Nationality", f"{row['Nationality']}")
            province_id = st.text_input("Province_Id", f"{row['Province_Id']}")
            school_id = st.text_input("School_Id", f"{row['School_Id']}")
            class_name = st.text_input("Class_Name", f"{row['Class_Name']}")
            phone = st.text_input("Phone", f"{row['Phone']}")
            email = st.text_input("Email", f"{row['Email']}")
            address = st.text_input("Address", f"{row['Address']}")
            ad_exam_results = st.text_input("Ad_Exam_Results", f"{row['Ad_Exam_Results']}")
            hs_edu = st.text_input("HS_Edu", f"{row['HS_Edu']}")
            continuing_edu = st.text_input("Continuing_Edu", f"{row['Continuing_Edu']}")
            status = st.text_input("Status", f"{row['Status']}")
            update_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            break
        if st.button("Lưu"):
            # Thực hiện xử lý lưu dữ liệu vào cơ sở dữ liệu hoặc tệp tin
            query_update = f"UPDATE {table_name} SET ID = N'{id}',FirstName = N'{first_name}',LastName = N'{last_name}',Gender = N'{gender}',DateB = N'{date_b}',MonthB = N'{month_b}',YearB = N'{year_b}',Birthplace = N'{birthplace}',Nationality = N'{nationality}',ProvinceId = N'{province_id}',SchoolId = N'{school_id}',ClassName = N'{class_name}',Phone = N'{phone}',Email = N'{email}',Address = N'{address}',AdExamResults = N'{ad_exam_results}',HSEdu = N'{hs_edu}',ContinuingEdu = N'{continuing_edu}',Status = N'{status}',UpdateAt = N'{update_date}' WHERE CitizenId = '{changeData}'"
            CURSOR.execute(query_update)
            CONN.commit()
            if CURSOR.rowcount >0: 
                dtf = Datamanagerment(config, CURSOR,pd,table_name,search_query)
                st.success("Dữ liệu đã được lưu thành công!")
            else:
                st.success("Dữ liệu lưu thất bại!")
                

    st.title("Xóa Dữ Liệu")
    
    row_index = st.text_input("Xóa dữ liệu dựa vào Citizen_Id : ", "")

   
    if st.button("Xóa"):
        # Xóa hàng khỏi DataFrame
        # st.warning("Bạn có chắc chắn muốn thực hiện xóa?")
        
        # if st.button("Xác nhận"):
        if int(len(row_index)) == 12 and row_index.isdigit() == True:
            query_delete = f"DELETE FROM {table_name} WHERE CitizenId like '{row_index}'"
            CURSOR.execute(str(query_delete))
            CONN.commit()
            # CURSOR.close()
            # CONN.close()
           
            
            if CURSOR.rowcount > 0:
               
                dtf = Datamanagerment(config, CURSOR,pd,table_name, search_query)
                st.success("Dữ liệu đã được xóa thành công!")

            else:
                st.warning("Dữ liệu đã chưa được xóa!")
        else:
            st.warning("Citizen Id không đúng")
        # elif st.button("Hủy"):
        #     st.warning("Thao tác xóa đã bị hủy bỏ.")

        

def convertExcelToDb(df, cursor, conn, config, columns) :
    COUNT_ROW_DATA = 4
    COUNT_NAME_TABLE = 2
    COUNT_ID = 1
    # Lấy số lượng phần tử trong file Excel
    COLUMN_NUMBER = int(df.shape[0])

    table_name = config.get('table', 'table_name')
    #Truy xuất số lượng dòng trong bảng
    column_name = config.get('column', 'column1')
    queryCount = f"SELECT MAX({column_name}) FROM {table_name}"
    cursor.execute(queryCount)
    # Lấy giá trị cao nhất từ kết quả truy vấn
    max_value = cursor.fetchone()[0]
    if max_value is not None :
        COUNT_ID = int(max_value)
    else: 
        COUNT_ID = 1

    # Truy xuất tên trường dữ liệu trong table

    
    query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
    cursor.execute(query)

    
        
    # Lấy danh sách tên các trường dữ liệu từ kết quả truy vấn
    name_table_columns = [row.COLUMN_NAME for row in cursor.fetchall()]

    sql_Query = f"INSERT INTO {table_name} ("
    
    try:
        while True:
            if COUNT_NAME_TABLE < len(name_table_columns):
                sql_Query = sql_Query + name_table_columns[COUNT_NAME_TABLE-1]+","
            elif COUNT_NAME_TABLE == len(name_table_columns):
                sql_Query = sql_Query + name_table_columns[COUNT_NAME_TABLE-1]+") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            else:
                break
            COUNT_NAME_TABLE += 1
        
      

        Data_already_exists = {'ID': [],'First_Name': [],'Last_Name': [],'Gender': [],'Date_B': [],'Month_B': [],'Year_B': [],'Birthplace': [],'Nationality': [],'Citizen_Id': [],'Province_Id': [],'School_Id': [],'Class_Name': [],'Phone': [],'Email': [],'Address': [],'Ad_Exam_Results': [],'HS_Edu': [],'Continuing_Edu': [],
        }

        

        while COUNT_ROW_DATA < COLUMN_NUMBER :
            
            query = f"SELECT * FROM {table_name} WHERE CitizenId = '{ str(df.loc[COUNT_ROW_DATA, [columns[9]]].iloc[0]) }'"
            cursor.execute(query)
            row = cursor.fetchone()
            
            if not row:
                count1 = "TH00"+str(COUNT_ID)
                cursor.execute(sql_Query,
                        count1,
                        str(df.loc[COUNT_ROW_DATA, [columns[1]]].iloc[0]),
                        str(df.loc[COUNT_ROW_DATA, [columns[2]]].iloc[0]),
                        str(df.loc[COUNT_ROW_DATA, [columns[3]]].iloc[0]),
                        str(df.loc[COUNT_ROW_DATA, [columns[4]]].iloc[0]),
                        str(df.loc[COUNT_ROW_DATA, [columns[5]]].iloc[0]),
                        str(df.loc[COUNT_ROW_DATA, [columns[6]]].iloc[0]),
                        str(df.loc[COUNT_ROW_DATA, [columns[7]]].iloc[0]),
                        str(df.loc[COUNT_ROW_DATA, [columns[8]]].iloc[0]),
                        str(df.loc[COUNT_ROW_DATA, [columns[9]]].iloc[0]),
                        str(df.loc[COUNT_ROW_DATA, [columns[10]]].iloc[0]),
                        str(df.loc[COUNT_ROW_DATA, [columns[11]]].iloc[0]),
                        str(df.loc[COUNT_ROW_DATA, [columns[12]]].iloc[0]),
                        str(df.loc[COUNT_ROW_DATA, [columns[13]]].iloc[0]),
                        str(df.loc[COUNT_ROW_DATA, [columns[14]]].iloc[0]),
                        str(df.loc[COUNT_ROW_DATA, [columns[15]]].iloc[0]),
                        str(df.loc[COUNT_ROW_DATA, [columns[16]]].iloc[0]),
                        str(df.loc[COUNT_ROW_DATA, [columns[17]]].iloc[0]),
                        str(df.loc[COUNT_ROW_DATA, [columns[18]]].iloc[0]),
                        str("None"),
                        datetime.datetime.now(),
                        datetime.datetime.now()
                        )
                conn.commit()
            else:
                DataAlreadyExists(Data_already_exists, df, COUNT_ROW_DATA,columns)
            
            COUNT_ID = int(COUNT_ID) + 1
            COUNT_ROW_DATA += 1
        st.write("Data already exists.")
        dtf = pd.DataFrame(Data_already_exists)
        st.dataframe(dtf)
        cursor.close()
        conn.close()
        logging.info('Data adding successfully.')
        return True 
    except po.Error as e:
        logging.info(f'Data error. {e}')
        return False


def VerifyData(df) :
    CHECK = ['STT', 'Họ tên', 'Tên', 'Giới tính', 'Ngày tháng năm sinh ', 'Nơi sinh', 'Dân tộc', 'CMND', 'Tên Lớp 12', 'Điện thoại', 'Email', 'Địa chỉ liên hệ', 'Thí sinh dùng kết quả thi để xét tuyển sinh ĐH, CĐ', 'Hình thức giáo dục phổ thông']
    # Lấy danh sách các cột tiêu đề
    columnsEx = df.loc[1]
    VerifyList = []
    # In danh sách các cột tiêu đề
    for column in columnsEx:
        if str(column) != 'nan':
            VerifyList.append(column)
    
    if VerifyList == CHECK:
        return True
    else:
        return False
    
        

def main():
    config = configparser.ConfigParser()
    config.read('./cofig.ini')

    username = config.get('database', 'username')
    password = config.get('database', 'password')
    database = config.get('database', 'database_name')
    server = config.get('database', 'server')

    Connection = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    logging.basicConfig(filename='./logs/streamlit.log', level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

    

    try:
        CONN = po.connect(Connection)
        # Tạo đối tượng cursor
        CURSOR = CONN.cursor()
        logging.info('Streamlit APP running.')
        st.set_page_config(page_title="Convert Excel to Database")

        st.markdown(
            """
            <style>
            .footer {
                position: fixed;
                left: 0;
                bottom: 0;
                width: 100vw;
                background-color: #ffffff;
                color: #555;
                padding: 10px;
            };
            .url {
                text-decoration:None;
                color:#555;
            }
            </style>
            """,
            unsafe_allow_html=True
        )

        def pageExcel():
            st.header("Upload Your Excel")
            excel_file = st.file_uploader("Upload your Excel", type="xlsx")
            if excel_file is not None:
                excel_columns = []
                df = pd.read_excel(excel_file)
                st.write("Uploading...")
                check_data = VerifyData(df)
                if  check_data == True:
                    # st.dataframe(df.columns)
                    for item in df.columns:
                        excel_columns.append(item)
                    
                    result = convertExcelToDb(df, CURSOR, CONN, config,excel_columns)

                    if result == True:
                        st.write(f"Upload Successfully")
                    else:
                        st.write(f"Upload Fail")
                else:
                    st.write(f"Format Excel Error.")
                    logging.info(f'Data Excel error.')

        def pageManagerment():
            css = """
                <style>
                div.stTextInput > div > div > input {
                    border-color: #000000 !important;
        
                }
                </style>
                """
            st.markdown(css, unsafe_allow_html=True)
            
            st.header("Student Management List")
            search_query = st.text_input("Tìm kiếm (Tìm kiếm theo số CCCD sinh viên): ", "", help="Nhập thông tin vào ngay bên dưới...")
            
            ChangeAndDelete(st,CURSOR,search_query,config,pd,CONN)


       
        
        # Tạo sidebar ở dưới cùng với nội dung footer
        st.sidebar.title("Viễn Đông Students Management")
        st.sidebar.markdown("---")
        selection = st.sidebar.selectbox("Chọn trang", ("Thêm sinh viên qua Excel", "Quản lý sinh viên")) 
        if selection == "Thêm sinh viên qua Excel":
            pageExcel()
        else:
            pageManagerment()
    except po.Error as e:
        st.write(f"Lỗi kết nối SQL Server: {e}")
        logging.info(f'SQL error. {e}')
    st.markdown("""<div class="footer">
        Viễn Đông Collage </br>
        Made with <a class="url" href="https://streamlit.io/">Streamlit </a>
    </div>""",unsafe_allow_html=True)
if __name__ == '__main__':
    main()