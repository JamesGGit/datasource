import urllib.request
import os.path
import zipfile
import pandas as pd

INDEX_FILE_TEMPLATE_URL = 'https://www.sec.gov/Archives/edgar/full-index/{year_num}/QTR{quarter_num}/company.zip'
LOCAL_DOWNLOAD_LOCATION = 'C:/Data/SEC'
CIK_MAPPING_FILE_PATH_TEMPLATE= LOCAL_DOWNLOAD_LOCATION + '/{year_num}_QTR{quarter_num}_CIK_mapping.csv'


def unzip_file(filepath,year,quarter_num):
    zfile = zipfile.ZipFile(filepath)
    desired_file_name = 'company.idx'
    unzipped_output_zip_file = os.path.join(LOCAL_DOWNLOAD_LOCATION,
                                            '{year_num}_QTR{quarter_num}'.format(year_num=year,quarter_num=quarter_num)
                                            + desired_file_name)
    for finfo in zfile.infolist():
        if finfo.filename == desired_file_name:
            with zfile.open(finfo) as ifile:
                with open(unzipped_output_zip_file,'wb') as fout:
                    fout.write(ifile.read())

    return unzipped_output_zip_file


def download_index_file(year, quarter_num):
    file_name = '{year_num}_QTR{quarter_num}_company.zip'.format(year_num=year,quarter_num=quarter_num)
    url = INDEX_FILE_TEMPLATE_URL.format(year_num=year,quarter_num=quarter_num)
    print("downloading from url "+url )
    download_file_name = os.path.join(LOCAL_DOWNLOAD_LOCATION, file_name)
    if not os.path.isfile(download_file_name):
        urllib.request.urlretrieve(url, download_file_name)
    else:
        print("Skipping download as file already exists")

    return download_file_name


def get_specific_line_from_file(file_name,line_needed):
    with open(file_name) as cif:
        counter = 0
        while counter < line_needed:
            counter += 1
            header_line = cif.readline()
    return header_line


def build_edgar_cik_mappings(company_index_file, output_file):
    header_line = get_specific_line_from_file(company_index_file,9)
    max_length = len(get_specific_line_from_file(company_index_file,10))
    col_names = ['Company Name', 'Form Type', 'CIK', 'Date Filed', 'File Name']
    col_specs = [(header_line.index(col_name),header_line.index(col_name)+len(col_name)) for col_name in col_names]
    corrected_col_spec = []
    for i in range(len(col_specs)):
        if i < len(col_specs) - 1:
            corrected_col_spec.append((col_specs[i][0], col_specs[i + 1][0] - 1))
        else:
            corrected_col_spec.append((col_specs[i][0], max_length))

    df_cik_listing_mapping = pd.read_fwf(company_index_file,skiprows=10,colspecs=corrected_col_spec,index_col=False,names=['Company Name','Form Type','CIK','Date Filed','File Name'])
    df_cik_listing_mapping.to_csv(output_file,index=False)
    df_comp_name_cik = df_cik_listing_mapping[['Company Name','CIK']]
    return df_comp_name_cik.drop_duplicates()


if __name__ == '__main__':
    year = 2018
    quarter_num = 1

    download_file_name = download_index_file(year,quarter_num)
    unzipped_output_zip_file = unzip_file(download_file_name,year,quarter_num)
    OUTPUT_CIK_MAPPING_FILE = CIK_MAPPING_FILE_PATH_TEMPLATE.format(year_num=year, quarter_num=quarter_num)
    company_cik_map = build_edgar_cik_mappings(unzipped_output_zip_file, OUTPUT_CIK_MAPPING_FILE)
    print(company_cik_map)

