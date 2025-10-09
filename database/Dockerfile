FROM postgres:15

# Copy initialization scripts
COPY db.sql /docker-entrypoint-initdb.d/

# Set environment variables
ENV POSTGRES_USER=admin
ENV POSTGRES_PASSWORD=admin
ENV POSTGRES_DB=defence_db

EXPOSE 5432
