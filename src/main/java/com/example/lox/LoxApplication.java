package com.example.lox;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.jdbc.lock.DefaultLockRepository;
import org.springframework.integration.jdbc.lock.JdbcLockRegistry;
import org.springframework.integration.jdbc.lock.LockRepository;
import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

@SpringBootApplication
public class LoxApplication {

	public static void main(String[] args) {
		SpringApplication.run(LoxApplication.class, args);
	}

	@Bean
	DefaultLockRepository defaultLockRepository(DataSource dataSource) {
		return new DefaultLockRepository(dataSource);
	}

	@Bean
	JdbcLockRegistry jdbcLockRegistry(LockRepository repository) {
		return new JdbcLockRegistry(repository);
	}
}


@RestController
@Log4j2
@RequiredArgsConstructor
class LockedResourceController {

	private final LockRegistry registry;
	private final ReservationRepository repository;

	@GetMapping("/update/{id}/{name}/{time}")
	Reservation update(@PathVariable int id,
																				@PathVariable String name,
																				@PathVariable long time) throws Exception {

		Lock lock = registry.obtain(Integer.toString(id));
		boolean acquired = lock.tryLock(1, TimeUnit.SECONDS);
		if (acquired) {
			try {
				log.info("looking for record # " + id);
				this.repository.findById(id).ifPresent(r -> {
					r.setName(name);
					repository.save(r);
				});
				Thread.sleep(time);
			}
			catch (Exception e) {
				ReflectionUtils.rethrowRuntimeException(e);
			}
			finally {
				lock.unlock();
			}
		}
		return this.repository.findById(id).orElseThrow(() -> new IllegalArgumentException("no matching record!"));
	}


}

@Service
@RequiredArgsConstructor
class ReservationRepository {

	private final JdbcTemplate template;
	private final RowMapper<Reservation> mapper = (resultSet, i) -> new Reservation(resultSet.getInt("id"), resultSet.getString("name"));

	Optional<Reservation> findById(int id) {
		var reservations = this.template
			.query("select * from reservation where id =? ", this.mapper, id);
		Iterator<Reservation> iterator = reservations.iterator();
		if (iterator.hasNext()) {
			return Optional.ofNullable(iterator.next());
		}
		return Optional.empty();
	}

	Collection<Reservation> findAll() {
		return this.template.query("select * from reservation ", this.mapper);
	}

	Reservation save(Reservation r) {
		return this
			.template
			.execute("update reservation set name = ? where id =? ", (PreparedStatement ps) -> {
				ps.setString(1, r.getName());
				ps.setInt(2, r.getId());
				ps.execute();
				return findById(r.getId()).get();
			});
	}
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Reservation {
	private Integer id;
	private String name;
}