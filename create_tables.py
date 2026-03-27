from database import engine
import models   # 👈 full file import

print("Tables detected:", models.Base.metadata.tables.keys())

models.Base.metadata.create_all(bind=engine)

print("Done")