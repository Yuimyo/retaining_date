CREATE TABLE dir_props (
    id INTEGER primary key autoincrement,
    path TEXT
);

CREATE TABLE dir_actions_log (
    id INTEGER primary key autoincrement,
    dir_id INTEGER,
    action_type INTEGER,
    cached_date TEXT
);

CREATE TABLE dir_file_props (
    id INTEGER primary key autoincrement,
    dir_id INTEGER,
    name TEXT,
    cached_date TEXT,
    created_date TEXT,
    modified_date TEXT
);