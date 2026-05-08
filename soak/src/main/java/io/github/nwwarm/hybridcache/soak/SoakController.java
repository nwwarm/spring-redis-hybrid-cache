package io.github.nwwarm.hybridcache.soak;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/cache")
public class SoakController {

    private final SoakService service;

    public SoakController(SoakService service) {
        this.service = service;
    }

    @GetMapping("/products/{id}")
    public String product(@PathVariable long id) {
        return service.findProduct(id);
    }

    @GetMapping("/users/{id}")
    public Mono<String> user(@PathVariable long id) {
        return service.findUser(id);
    }

    @GetMapping("/sessions/{token}")
    public String session(@PathVariable String token) {
        return service.findSession(token);
    }

    @DeleteMapping("/products")
    public String clear() {
        service.clearProducts();
        return "ok";
    }
}
