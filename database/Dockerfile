services:
  defence_db:
    image: postgres:15
    container_name: defence_db
    environment:
      POSTGRES_USER: sujith
      POSTGRES_PASSWORD: ""
      POSTGRES_DB: dsp_project
    ports:
      - "5432:5432"
    volumes:
      - pg_data:/var/lib/postgresql/data

  defence_fastapi:
    build: ./fastapi
    container_name: defence_fastapi
    depends_on:
      - defence_db
    environment:
      DATABASE_URL: postgresql+psycopg2://sujith:@defence_db:5432/dsp_project

volumes:
  pg_data:
