import joblib

# Lazy import to avoid circular import
def get_imported_id():
    from Intrusion_identification.imported_identification import ImportedID
    return ImportedID

def get_manual_id():
    from Intrusion_identification.manual_identification import ManualID
    return ManualID

ImportedID = get_imported_id()
# Create an instance of ImportedID
instance = ImportedID(dataset=None)  # Replace with a valid dataset if needed

# Test serialization
joblib.dump(instance, 'test.pkl')

# Test deserialization
loaded_instance = joblib.load('test.pkl')

print(f"Original: {instance}")
print(f"Loaded: {loaded_instance}")