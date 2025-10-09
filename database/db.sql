-- Table to store predictions
CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    customer_id INT,
    feature_1 FLOAT,
    feature_2 FLOAT,
    feature_3 FLOAT,
    geography VARCHAR(50),
    gender VARCHAR(10),
    prediction FLOAT,
    source VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table to store data quality issues
CREATE TABLE IF NOT EXISTS data_issues (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(255),
    issue_type VARCHAR(50),
    criticality VARCHAR(20),
    description TEXT,
    nb_rows INT,
    nb_valid INT,
    nb_invalid INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
