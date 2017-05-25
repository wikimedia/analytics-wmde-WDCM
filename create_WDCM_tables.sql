/* Creation of wdcm_datatable */
/* in database: u16664__wdcm_p */

CREATE TABLE wdcm_datatable (
  
  -- primary key
  id int NOT NULL AUTO_INCREMENT,
  
  -- item ID
  en_id varchar(255) NOT NULL,
  
  -- item aspect
  en_aspect varchar(37) NOT NULL,
  
  -- count
  en_count int,
  
  -- category
  en_category varchar(255) NOT NULL,
  
  -- project
  en_project varchar(255) NOT NULL,
  
  PRIMARY KEY (id)
);

CREATE INDEX ix_en_id ON wdcm_datatable (en_id);
CREATE INDEX ix_en_aspect ON wdcm_datatable (en_aspect);
CREATE INDEX ix_en_category ON wdcm_datatable (en_category);
CREATE INDEX ix_en_project ON wdcm_datatable (en_project);
CREATE INDEX ix_en_id_project ON wdcm_datatable (en_id, en_project);

