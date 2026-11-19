CREATE TABLE pin_resources (
    provider TEXT NOT NULL,
    cid TEXT NOT NULL,
    source_cid TEXT,
    logical_size BIGINT NOT NULL CHECK (logical_size >= 0),
    status TEXT NOT NULL,
    action TEXT,
    handle TEXT,
    claim_token TEXT,
    claim_until BIGINT,
    next_at BIGINT NOT NULL DEFAULT 0,
    attempt INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (provider, cid)
);
CREATE TABLE pin_refs (
    owner_object_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    cid TEXT NOT NULL,
    source TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    content_mode TEXT NOT NULL,
    expires_at BIGINT NOT NULL,
    remaining_providers TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (owner_object_id, provider, cid, source, policy_id, content_mode),
    FOREIGN KEY (provider, cid) REFERENCES pin_resources(provider, cid) ON DELETE CASCADE
);
CREATE INDEX pin_refs_demand ON pin_refs(provider,cid,expires_at);
CREATE INDEX pin_refs_expiry ON pin_refs(expires_at);
CREATE INDEX pin_refs_target ON pin_refs(cid,expires_at);
CREATE INDEX pin_resources_due ON pin_resources(provider,status,next_at);
CREATE INDEX pin_resources_capacity ON pin_resources(provider) WHERE status!='cleaned';
