package org.example.web.experiment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.example.web.experiment.ExperimentModels.Experiment;
import static org.example.web.experiment.ExperimentModels.Status;
import static org.example.web.experiment.ExperimentModels.Variant;

@Repository
public class ExperimentRepository {

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public ExperimentRepository(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    public List<Experiment> findAll() {
        return jdbc.query(
                "SELECT * FROM experiments ORDER BY created_at",
                (rs, rowNum) -> mapExperiment(rs)
        );
    }

    public Optional<Experiment> findById(UUID id) {
        return jdbc.query("SELECT * FROM experiments WHERE id = ?", (rs, rowNum) -> mapExperiment(rs), id)
                .stream().findFirst();
    }

    public Optional<Experiment> findRunningByKey(String key) {
        return jdbc.query(
                "SELECT * FROM experiments WHERE experiment_key = ? AND status = 'RUNNING'",
                (rs, rowNum) -> mapExperiment(rs),
                key
        ).stream().findFirst();
    }

    public Experiment insert(UUID id, ExperimentModels.ExperimentRequest request, String salt) {
        jdbc.update("""
                INSERT INTO experiments
                    (id, experiment_key, name, status, salt, traffic_percent)
                VALUES (?, ?, ?, 'DRAFT', ?, ?)
                """, id, request.key(), request.name(), salt, request.trafficPercent());
        replaceVariants(id, request.variants());
        return findById(id).orElseThrow();
    }

    public Experiment update(UUID id, ExperimentModels.ExperimentRequest request, String salt) {
        jdbc.update("""
                UPDATE experiments
                SET experiment_key = ?, name = ?, salt = ?, traffic_percent = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """, request.key(), request.name(), salt, request.trafficPercent(), id);
        replaceVariants(id, request.variants());
        return findById(id).orElseThrow();
    }

    public Optional<Experiment> setStatus(UUID id, Status status) {
        jdbc.update("""
                UPDATE experiments SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
                """, status.name(), id);
        return findById(id);
    }

    private void replaceVariants(UUID experimentId, List<ExperimentModels.VariantInput> variants) {
        jdbc.update("DELETE FROM experiment_variants WHERE experiment_id = ?", experimentId);
        for (ExperimentModels.VariantInput variant : variants) {
            JsonNode payload = variant.payload() == null
                    ? objectMapper.createObjectNode()
                    : variant.payload();
            jdbc.update("""
                    INSERT INTO experiment_variants
                        (id, experiment_id, variant_key, name, weight, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?::jsonb)
                    """, UUID.randomUUID(), experimentId, variant.key(), variant.name(),
                    variant.weight(), payload.toString());
        }
    }

    private Experiment mapExperiment(ResultSet rs) throws SQLException {
        UUID id = rs.getObject("id", UUID.class);
        Instant created = rs.getTimestamp("created_at").toInstant();
        Instant updated = rs.getTimestamp("updated_at").toInstant();
        List<Variant> variants = jdbc.query("""
                SELECT id, variant_key, name, weight, payload_json::text AS payload
                FROM experiment_variants
                WHERE experiment_id = ?
                ORDER BY variant_key
                """, (variantRs, rowNum) -> new Variant(
                variantRs.getObject("id", UUID.class),
                variantRs.getString("variant_key"),
                variantRs.getString("name"),
                variantRs.getInt("weight"),
                readJson(variantRs.getString("payload"))
        ), id);
        return new Experiment(
                id,
                rs.getString("experiment_key"),
                rs.getString("name"),
                Status.valueOf(rs.getString("status")),
                rs.getString("salt"),
                rs.getInt("traffic_percent"),
                created,
                updated,
                variants
        );
    }

    private JsonNode readJson(String value) {
        try {
            return objectMapper.readTree(value);
        } catch (Exception ex) {
            throw new IllegalStateException("invalid variant payload in database", ex);
        }
    }
}
