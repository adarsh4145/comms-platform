package io.github.adarsh4145.dispatchService.repository;

import io.github.adarsh4145.dispatchService.domain.DeadLetterEvent;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DeadLetterEventRepository extends JpaRepository<DeadLetterEvent, UUID> {

  List<DeadLetterEvent> findByStatusOrderByCreatedAtDesc(
      DeadLetterEvent.Status status, Pageable pageable);

  List<DeadLetterEvent> findByTraceId(String traceId);
}
