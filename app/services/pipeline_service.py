class PipelineService:
    @staticmethod
    def process_uploaded_file(file_path: str, extension: str):
        # Заглушка
        if extension == ".csv":
            print(f"CSV file accepted for processing: {file_path}")
        elif extension == ".xlsx":
            print(f"XLSX file accepted for processing: {file_path}")
        else:
            print(f"Unsupported file type: {file_path}")
