CREATE FUNCTION update_updated_column() RETURNS trigger AS $$
    BEGIN
        NEW.updated = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER wallet_updated
BEFORE UPDATE ON wallets
FOR EACH ROW
EXECUTE FUNCTION update_updated_column();
