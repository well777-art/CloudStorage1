# Flask components to set up the application, handle requests, render templates, and return JSON responses
from flask import Flask, request, render_template, jsonify, send_file
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import os
import datetime
import pyodbc
from time import sleep
import zipfile

# Initialize a new Flask application instance.
app = Flask(__name__)

# Azure Blob Storage connection
connect_str = "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=bksdevstore202411060719;AccountKey=M+uK3jdfhvyVzWU9rs1frx+tGvTgw2wc4bQlIiFpYXdg2S4gQxX5fKkORpDLmmBmFTC7cevZDIxA+AStnuEjsQ==;BlobEndpoint=https://bksdevstore202411060719.blob.core.windows.net/;FileEndpoint=https://bksdevstore202411060719.file.core.windows.net/;QueueEndpoint=https://bksdevstore202411060719.queue.core.windows.net/;TableEndpoint=https://bksdevstore202411060719.table.core.windows.net/"
container_name = "uploads"

# Initialize the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_name)

# Azure SQL Database connection string
server = 'bks-sql-server.database.windows.net'
database = 'bks-database'
username = 'bkssql'
password = 'bks@1234'
connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

def get_db_connection():
    """
    Establishes and returns a new database connection and cursor.
    """
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    return conn, cursor

@app.route('/')
def home():
    """
    Renders the homepage for the file upload service.
    """
    return render_template('upload.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """
    Handles multiple file uploads to Azure Blob Storage and stores metadata in the database.
    """
    if 'file' not in request.files:
        return jsonify({"message": "No file part in the request"}), 400

    files = request.files.getlist('file')  # Retrieve multiple files for batch upload
    if not files:
        return jsonify({"message": "No selected files"}), 400

    uploaded_files = []
    failed_files = []

    for file in files:
        if file.filename == '':
            continue  # Skip empty filenames

        try:
            file_name = file.filename
            upload_time = datetime.datetime.now()

            # Upload file to Blob Storage
            blob_client = container_client.get_blob_client(file_name)
            blob_client.upload_blob(file, overwrite=True)

            # Insert file metadata into the database
            conn, cursor = get_db_connection()
            cursor.execute("INSERT INTO FileMetadata (FileName, UploadTime) VALUES (?, ?)", (file_name, upload_time))
            conn.commit()
            uploaded_files.append(file_name)

        except Exception as e:
            failed_files.append(f"{file.filename}: {str(e)}")

        finally:
            if 'conn' in locals():
                conn.close()

    message = f"Files uploaded successfully: {', '.join(uploaded_files)}"
    if failed_files:
        message += f". Errors: {', '.join(failed_files)}"
    return jsonify({"message": message}), 200 if not failed_files else 207

@app.route('/get_uploads', methods=['GET'])
def get_uploads():
    """
    Retrieves all uploaded files from the database and returns as JSON.
    """
    try:
        for attempt in range(3):
            try:
                conn, cursor = get_db_connection()
                cursor.execute("""
                    SELECT FileName,
                    CAST(UploadTime AT TIME ZONE 'UTC' AT TIME ZONE 'India Standard Time' AS datetime) AS UploadTimeIST
                    FROM FileMetadata ORDER BY UploadTime DESC
                """)
                uploads = [{"file_name": row[0], "upload_time": row[1].strftime("%Y-%m-%d %H:%M:%S")} for row in cursor.fetchall()]
                return jsonify(uploads=uploads), 200
            except Exception as db_error:
                if attempt == 2:
                    raise db_error
                sleep(1)
    except Exception as e:
        return jsonify(error=str(e)), 500
    finally:
        if 'conn' in locals():
            conn.close()

@app.route('/delete', methods=['POST'])
def delete_file():
    """
    Deletes specified files from Blob Storage and the database.
    """
    file_names = request.json.get('file_names')
    if not file_names:
        return jsonify({"message": "File names not provided"}), 400

    deleted_files = []
    errors = []
    try:
        for file_name in file_names:
            try:
                blob_client = container_client.get_blob_client(file_name)
                blob_client.delete_blob()

                conn, cursor = get_db_connection()
                cursor.execute("DELETE FROM FileMetadata WHERE FileName = ?", (file_name,))
                conn.commit()
                deleted_files.append(file_name)
            except Exception as e:
                errors.append(f"Failed to delete {file_name}: {str(e)}")

        message = f"Files deleted successfully: {', '.join(deleted_files)}"
        if errors:
            message += f". Errors: {', '.join(errors)}"
        return jsonify({"message": message}), 200 if not errors else 207

    finally:
        if 'conn' in locals():
            conn.close()

@app.route('/download', methods=['POST'])
def download_files():
    """
    Downloads specified files from Blob Storage.
    - Single file: direct file download
    - Multiple files: ZIP format
    """
    file_names = request.json.get('file_names')
    if not file_names:
        return jsonify({"message": "File names not provided"}), 400

    try:
        if len(file_names) == 1:
            # Single file download
            file_name = file_names[0]
            blob_client = container_client.get_blob_client(file_name)
            file_data = BytesIO(blob_client.download_blob().readall())
            return send_file(file_data, as_attachment=True, download_name=file_name, mimetype='application/octet-stream')

        else:
            # Multiple files in a ZIP
            zip_buffer = BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
                for file_name in file_names:
                    blob_client = container_client.get_blob_client(file_name)
                    file_data = blob_client.download_blob().readall()
                    zip_file.writestr(file_name, file_data)

            zip_buffer.seek(0)
            return send_file(zip_buffer, as_attachment=True, download_name="files.zip", mimetype='application/zip')

    except Exception as e:
        print(f"Download error: {e}")
        return jsonify({"message": "An error occurred while downloading the files."}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
