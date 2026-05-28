use std::{fs, path::Path};

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("testing crate lives under crates/testing")
}

#[test]
fn issue_61_sdk_release_contract_is_wired() {
    let root = repo_root();
    let changeset_config =
        fs::read_to_string(root.join(".changeset/config.json")).expect("read changeset config");
    let root_package = fs::read_to_string(root.join("package.json")).expect("read package.json");
    let publish_workflow = fs::read_to_string(root.join(".github/workflows/sdk-publish.yml"))
        .expect("read SDK publish workflow");
    let pr_workflow =
        fs::read_to_string(root.join(".github/workflows/pr.yml")).expect("read PR workflow");
    let sdk_package =
        fs::read_to_string(root.join("packages/sdk/package.json")).expect("read SDK package");
    let generated_package = fs::read_to_string(root.join("packages/sdk-generated/package.json"))
        .expect("read generated SDK package");
    let sdk_readme =
        fs::read_to_string(root.join("packages/sdk/README.md")).expect("read SDK README");
    let sdk_changelog =
        fs::read_to_string(root.join("packages/sdk/CHANGELOG.md")).expect("read SDK changelog");
    let generated_changelog = fs::read_to_string(root.join("packages/sdk-generated/CHANGELOG.md"))
        .expect("read generated SDK changelog");

    for expected in [
        "\"changelog\": \"@changesets/cli/changelog\"",
        "\"access\": \"public\"",
        "\"baseBranch\": \"main\"",
    ] {
        assert!(
            changeset_config.contains(expected),
            "Changesets config should contain `{expected}`"
        );
    }

    for expected in [
        "\"@changesets/cli\"",
        "\"changeset\": \"changeset\"",
        "\"version-packages\": \"changeset version\"",
        "\"release:sdk\": \"changeset publish\"",
    ] {
        assert!(
            root_package.contains(expected),
            "root package should wire Changesets script/dependency `{expected}`"
        );
    }

    for expected in [
        "name: Publish SDK",
        "branches:",
        "- main",
        "changesets/action@v1",
        "version: pnpm version-packages",
        "publish: pnpm release:sdk",
        "NPM_TOKEN: ${{ secrets.NPM_TOKEN }}",
        "pnpm turbo run build --filter=@au-kpis/sdk... --cache-dir=.turbo",
        "pnpm turbo run test --filter=@au-kpis/sdk --cache-dir=.turbo",
    ] {
        assert!(
            publish_workflow.contains(expected),
            "SDK publish workflow should contain `{expected}`"
        );
    }

    for runtime in ["node", "bun", "deno", "browser"] {
        assert!(
            pr_workflow.contains(&format!("- {runtime}"))
                || pr_workflow.contains(&format!("- {runtime}\n")),
            "PR workflow should keep SDK runtime coverage for `{runtime}`"
        );
    }

    assert_publishable_sdk_package(&sdk_package);
    assert_publishable_generated_package(&generated_package);

    for expected in [
        "npm install @au-kpis/sdk",
        "import { createClient } from '@au-kpis/sdk'",
        "Node 20+",
        "Bun",
        "Deno",
        "modern browsers",
        "client.observations.list",
        "client.dataflows.list",
    ] {
        assert!(
            sdk_readme.contains(expected),
            "SDK README quickstart should contain `{expected}`"
        );
    }

    for changelog in [sdk_changelog, generated_changelog] {
        assert!(
            changelog.contains("## 1.0.0"),
            "SDK package changelogs should include the generated v1.0.0 release"
        );
    }
}

fn assert_publishable_sdk_package(package: &str) {
    for expected in [
        "\"name\": \"@au-kpis/sdk\"",
        "\"version\": \"1.0.0\"",
        "\"types\": \"./dist/index.d.ts\"",
        "\"sideEffects\": false",
        "\"files\"",
        "\"dist/index.d.ts\"",
        "\"dist/client.js\"",
        "\"README.md\"",
        "\"CHANGELOG.md\"",
        "\"publishConfig\"",
        "\"access\": \"public\"",
        "\"engines\"",
        "\"node\": \">=20\"",
        "\"@au-kpis/sdk-generated\": \"workspace:^\"",
    ] {
        assert!(
            package.contains(expected),
            "SDK package should contain `{expected}`"
        );
    }

    assert!(
        !package.contains("\"private\": true"),
        "SDK package must be publishable"
    );
}

fn assert_publishable_generated_package(package: &str) {
    for expected in [
        "\"name\": \"@au-kpis/sdk-generated\"",
        "\"version\": \"1.0.0\"",
        "\"types\": \"./dist/index.d.ts\"",
        "\"sideEffects\": false",
        "\"files\"",
        "\"dist/index.d.ts\"",
        "\"dist/zod.js\"",
        "\"CHANGELOG.md\"",
        "\"publishConfig\"",
        "\"access\": \"public\"",
        "\"engines\"",
        "\"node\": \">=20\"",
    ] {
        assert!(
            package.contains(expected),
            "generated SDK package should contain `{expected}`"
        );
    }

    assert!(
        !package.contains("\"private\": true"),
        "generated SDK package must be publishable because the public SDK depends on it"
    );
}
