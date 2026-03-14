test:
	npx vitest --reporter=verbose --run

test-watch:
	npx vitest

types:
	npx tsc --declaration --allowJs --emitDeclarationOnly --skipLibCheck \
		--target es2020 --module nodenext --moduleResolution nodenext \
		--strict false --esModuleInterop true --outDir ./types src/index.js

types-clean:
	rm -rf types

up:
	docker compose up -d

down:
	docker compose down

down-volumes:
	docker compose down -v

logs:
	docker compose logs -f

clean:
	rm -rf node_modules

install:
	npm install
