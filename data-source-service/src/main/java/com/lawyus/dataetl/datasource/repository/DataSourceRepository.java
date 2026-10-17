package com.lawyus.dataetl.datasource.repository;

import com.lawyus.dataetl.datasource.entity.DataSource;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DataSourceRepository extends JpaRepository<DataSource, Long> {
}
